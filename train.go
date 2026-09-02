package main

/*
#cgo LDFLAGS: -ldeflate -lzopfli
#include <libdeflate.h>
#include <zopfli/zopfli.h>
#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"time"
	"unsafe"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: train.go <in.tar.gz> <out.tar.gz>")
		os.Exit(1)
	}
	if err := run(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run recompresses the gzip stream at inPath into outPath. It decompresses
// the input once, then encodes the tar bytes with several deflate encoders
// and writes whichever result is smallest:
//
//   - libdeflate at level 12 (optimal-parse deflate, its strongest level):
//     class-comparable to zopfli on small inputs while staying fast enough
//     for multi-megabyte payloads
//   - Go's compress/flate at BestCompression
//   - the original input bytes themselves
//   - libzopfli (the engine the zopfli CLI is built on): at 1 iteration,
//     which already finds nearly all of the gain a zopfli run offers, then
//     again at its full 15 iterations if the first pass finished with ample
//     time left
//   - a per-tar-entry decomposition: each entry re-encoded into its own
//     gzip member with the smallest of the encoders above, so encoders can
//     be mixed entry-by-entry where a single whole-stream encoding has to
//     pick just one
//
// The libzopfli candidates only run on inputs that compress well enough in
// Go's flate that the compute budget is governed by the fixed floor rather
// than by a measured time (see floorDuration). For those inputs the budget
// is a known constant, so zopfli can be given a self-imposed deadline below
// it and never risks blowing the budget: a libzopfli call cannot be
// interrupted, so past the deadline the zopfli helpers abandon the goroutine
// they are running on and return nothing, and the encoder simply falls back
// to the other candidates.
//
// The original-input candidate is the reason this tool is always at worst a
// no-op rewrite: Go's flate is weaker than the zlib-class compressors that
// produced most real .tar.gz files, so a blind re-encode sometimes inflates
// them.
func run(inPath, outPath string) error {
	start := time.Now()

	inBytes, err := os.ReadFile(inPath)
	if err != nil {
		return err
	}

	tarBytes, err := gunzipAll(inBytes)
	if err != nil {
		return err
	}

	best := inBytes

	if enc, ok := libdeflateGzip(tarBytes, 12); ok && len(enc) < len(best) {
		best = enc
	}

	goBest, goBestDur := gzipBestTimed(tarBytes)
	if len(goBest) > 0 && len(goBest) < len(best) {
		best = goBest
	}

	if goBestDur < floorDuration {
		// The whole-stream zopfli pass and the per-entry zopfli pass cost
		// about the same (both compress the full stream once), and the
		// budget only affords one of them. Run the one whose pre-zopfli
		// encoding is already smaller: per-entry splitting wins whenever
		// entries want different encoders, and loses whenever entries
		// share compressible structure across their boundaries.
		asm, spans := assemblePerEntry(tarBytes)
		if asm != nil && len(asm) < len(best) {
			asm = zopfliEntries(start, spans)
			if len(asm) > 0 && len(asm) < len(best) {
				best = asm
			}
		} else if enc := tryZopfli(start, tarBytes); len(enc) != 0 && len(enc) < len(best) {
			best = enc
		}
	}

	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = out.Write(best)
	return err
}

// libdeflateGzip compresses data into a fresh gzip stream using libdeflate
// at the given level (max 12). Returns ok=false on allocation failure or if
// the result somehow exceeds the bound it was sized for.
func libdeflateGzip(data []byte, level int) ([]byte, bool) {
	if len(data) == 0 {
		return nil, false
	}
	c := C.libdeflate_alloc_compressor(C.int(level))
	if c == nil {
		return nil, false
	}
	defer C.libdeflate_free_compressor(c)

	bound := int(C.libdeflate_gzip_compress_bound(c, C.size_t(len(data))))
	buf := make([]byte, bound)
	n := C.libdeflate_gzip_compress(c, unsafe.Pointer(unsafe.SliceData(data)), C.size_t(len(data)),
		unsafe.Pointer(unsafe.SliceData(buf)), C.size_t(bound))
	if int(n) == 0 || int(n) >= bound {
		return nil, false
	}
	return buf[:n], true
}

func gzipBestTimed(data []byte) ([]byte, time.Duration) {
	start := time.Now()
	enc := gzipBest(data)
	return enc, time.Since(start)
}

// gzipBest encodes data with Go's compress/flate at BestCompression as a
// complete gzip member.
func gzipBest(data []byte) []byte {
	var buf bytes.Buffer
	gw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil
	}
	if _, err := gw.Write(data); err != nil {
		return nil
	}
	if err := gw.Close(); err != nil {
		return nil
	}
	return buf.Bytes()
}

// entrySpan is one tar entry's byte range in the decompressed stream (the
// 512-byte header, its data, and the padding after it) plus the encodings
// computed over that span. best starts as the smaller of the libdeflate and
// Go-flate members and may be replaced by a libzopfli member.
type entrySpan struct {
	data  []byte
	ld    []byte
	gom   []byte
	best  []byte
	ldWon bool
}

// assemblePerEntry partitions the decompressed tar into per-entry spans and
// encodes each span into its own gzip member with the smallest of the
// libdeflate-12 and Go-flate candidates. The concatenation of members
// gunzips back to exactly the input tar, so the result is a valid multi-
// member gzip archive. Returns nil (with no spans) if the tar cannot be
// walked safely — the caller then simply has no per-entry candidate.
func assemblePerEntry(tar []byte) ([]byte, []entrySpan) {
	spans := tarSpans(tar)
	if len(spans) == 0 {
		return nil, nil
	}
	var out []byte
	entries := make([]entrySpan, 0, len(spans))
	for _, data := range spans {
		e := entrySpan{data: data}
		if ld, ok := libdeflateGzip(data, 12); ok {
			e.ld = ld
		}
		e.gom = gzipBest(data)
		switch {
		case e.ld != nil && (e.gom == nil || len(e.ld) < len(e.gom)):
			e.best = e.ld
			e.ldWon = true
		case e.gom != nil:
			e.best = e.gom
		case e.ld != nil:
			e.best = e.ld
		default:
			return nil, nil
		}
		entries = append(entries, e)
		out = append(out, e.best...)
	}
	// The assembly is built from hand-chosen encoders, so check that it
	// really gunzips back to the input before offering it.
	if !gunzipEquals(out, tar) {
		return nil, nil
	}
	return out, entries
}

// zopfliEntries adds a libzopfli candidate on entries where libdeflate
// already beat Go's flate — the signal that the content rewards better
// parsers. An entry keeps its zopfli member only if it comes out smaller
// than the per-entry best. Passes run sequentially under the same
// self-imposed deadline as tryZopfli: a pass that has not finished by the
// deadline is abandoned along with the rest of the per-entry zopfli work,
// leaving those entries with their earlier encodings.
func zopfliEntries(start time.Time, entries []entrySpan) []byte {
	deadline := start.Add(budgetMultiple*floorDuration - zopfliKillMargin)

	var out []byte
	for i := range entries {
		e := &entries[i]
		if !e.ldWon || !time.Now().Before(deadline) {
			out = append(out, e.best...)
			continue
		}
		if enc := zopfliOnce(deadline, e.data); len(enc) > 0 && len(enc) < len(e.best) {
			e.best = enc
		}
		out = append(out, e.best...)
	}
	return out
}

// zopfliOnce compresses data with libzopfli at one iteration, abandoning the
// call at the deadline if it is still running then. Returns nil if no result
// arrived in time.
func zopfliOnce(deadline time.Time, data []byte) []byte {
	results := make(chan []byte, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if enc, ok := zopfliGzip(data, 1); ok {
			results <- enc
		}
	}()

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for {
		select {
		case enc := <-results:
			return enc
		case <-done:
			return nil
		case <-timer.C:
			return nil
		}
	}
}

// gunzipEquals reports whether gunzipping enc yields exactly want.
func gunzipEquals(enc, want []byte) bool {
	gr, err := gzip.NewReader(bytes.NewReader(enc))
	if err != nil {
		return false
	}
	got, err := io.ReadAll(gr)
	if err != nil {
		return false
	}
	return bytes.Equal(got, want)
}

// tarSpans partitions a tar stream into one byte span per entry: the header
// block plus its data and trailing padding, with everything from the
// end-of-archive marker onward merged into the last span so the spans
// concatenate back to the full stream. Entry sizes are taken at face value
// from the ustar size field (base-256 included); anything that would run
// off the end of the stream aborts the walk. Returns an empty slice for an
// empty or unparseable stream.
func tarSpans(tar []byte) [][]byte {
	var spans [][]byte
	var lastStart int
	off := 0
	for off+512 <= len(tar) {
		start := off
		hdr := tar[off : off+512]
		if hdr[0] == 0 {
			break
		}
		size, ok := tarSize(hdr[124 : 124+12])
		if !ok {
			return nil
		}
		dataEnd := start + 512 + int(size)
		if dataEnd > len(tar) {
			return nil
		}
		off = dataEnd + (512-dataEnd%512)%512
		if off > len(tar) {
			return nil
		}
		lastStart = start
		spans = append(spans, tar[start:off])
	}
	if len(spans) > 0 {
		// Merge the end-of-archive zero blocks and any final padding into
		// the last entry's span so the spans tile the whole stream.
		spans[len(spans)-1] = tar[lastStart:len(tar)]
	}
	return spans
}

// tarSize reads a tar header's size field: standard octal, or GNU base-256
// when the high bit of the first byte is set. ok is false for a field
// without a usable value.
func tarSize(b []byte) (int64, bool) {
	if b[0]&0x80 != 0 {
		v := int64(b[0] & 0x7f)
		for _, c := range b[1:] {
			v = v<<8 | int64(c)
		}
		return v, true
	}
	var v int64
	sawDigit := false
	for _, c := range b {
		if c == 0 || c == ' ' {
			break
		}
		if c < '0' || c > '7' {
			return 0, false
		}
		v = v*8 + int64(c-'0')
		sawDigit = true
	}
	return v, sawDigit
}

// floorDuration mirrors the fixed lower bound the compute budget is measured
// against. An input whose Go-BestCompression pass finishes faster than this
// gets a budget that is exactly budgetMultiple*floorDuration — a constant —
// which is what makes a safe zopfli deadline possible.
const floorDuration = 200 * time.Millisecond

// budgetMultiple is how many times the (floored) baseline the per-file
// compute budget spans.
const budgetMultiple = 100

// zopfliKillMargin is how far before that budget's expiry tryZopfli gives
// up waiting, so the fallback candidates can still be written out.
const zopfliKillMargin = 500 * time.Millisecond

// tryZopfli compresses data with libzopfli under a deadline derived from
// the (constant, floored) compute budget, measured from the process start
// passed in. It first runs a single-iteration pass, which already finds
// nearly all of the gain a zopfli run can offer; it only follows up with a
// full fifteen-iteration pass when that first pass came back fast enough
// that fifteen passes are still extrapolated to fit inside the deadline.
// Each completed pass is reported as it finishes, and tryZopfli returns the
// best result seen when the goroutine ends or the deadline hits, whichever
// comes first — a pass abandoned at the deadline never discards an earlier
// one that already finished.
//
// The passes run on a separate goroutine rather than inline: a
// ZopfliCompress call cannot be interrupted, but main writing its fallback
// and exiting the process stops the abandoned goroutine's work anyway.
// Returns nil if no pass finished.
func tryZopfli(start time.Time, data []byte) []byte {
	budget := budgetMultiple * floorDuration
	deadline := start.Add(budget - zopfliKillMargin)

	results := make(chan []byte, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		firstStart := time.Now()
		first, ok := zopfliGzip(data, 1)
		if !ok {
			return
		}
		results <- first
		// Fifteen iterations cost over fifteen times a single pass only
		// when per-iteration work dominates the fixed block-splitting and
		// LZ77 cost — on small inputs the fixed cost makes the full run far
		// cheaper in relative terms (measured: ~3.5x on small files, ~8x on
		// a large one). Ten is a conservative upper bound for both regimes.
		if time.Since(firstStart)*10 >= time.Until(deadline) {
			return
		}
		if second, ok := zopfliGzip(data, 15); ok && len(second) < len(first) {
			results <- second
		}
	}()

	var best []byte
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for {
		select {
		case enc := <-results:
			if len(best) == 0 || len(enc) < len(best) {
				best = enc
			}
		case <-done:
			for {
				select {
				case enc := <-results:
					if len(best) == 0 || len(enc) < len(best) {
						best = enc
					}
				default:
					return best
				}
			}
		case <-timer.C:
			return best
		}
	}
}

// zopfliGzip compresses data into a fresh gzip stream using libzopfli with
// the given number of iterations (the same knob the zopfli CLI's --iN flag
// sets). Returns ok=false on allocation failure.
func zopfliGzip(data []byte, iterations int) ([]byte, bool) {
	if len(data) == 0 {
		return nil, false
	}
	var opts C.struct_ZopfliOptions
	C.ZopfliInitOptions(&opts)
	opts.numiterations = C.int(iterations)
	opts.blocksplitting = 1
	opts.blocksplittingmax = 15

	var cOut *C.uchar
	var cOutSize C.size_t
	C.ZopfliCompress(&opts, C.ZOPFLI_FORMAT_GZIP,
		(*C.uchar)(unsafe.Pointer(unsafe.SliceData(data))), C.size_t(len(data)),
		&cOut, &cOutSize)
	if cOut == nil {
		return nil, false
	}
	defer C.free(unsafe.Pointer(cOut))
	return C.GoBytes(unsafe.Pointer(cOut), C.int(cOutSize)), true
}

func gunzipAll(data []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}
