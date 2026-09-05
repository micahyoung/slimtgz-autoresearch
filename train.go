package main

/*
#cgo LDFLAGS: -ldeflate -lzopfli
#include <libdeflate.h>
#include <zopfli/zopfli.h>
#include <stdlib.h>
*/
import "C"

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
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
//   - libzopfli (the engine the zopfli CLI is built on) — either over the
//     whole stream or per tar entry, whichever side the pre-zopfli sizes
//     favor; the whole-stream side starts at one iteration and refines to
//     fifteen when the first pass left the budget's deadline far away
//   - a per-tar-entry decomposition: each entry re-encoded into its own
//     gzip member with the smallest of the encoders above, so encoders can
//     be mixed entry-by-entry where a single whole-stream encoding has to
//     pick just one
//
// The encoders above run over a re-serialized copy of the tar when that
// copy's libdeflate pass is already smaller than the raw tar's (see
// reserializeTar): old or hand-rolled archives carry header bytes that
// re-extract fine but compress worse than freshly written ones.
//
// The encodings are independent of one another, so they run on separate
// goroutines and the wall-clock cost is the slowest single encoding rather
// than the sum of all of them. The compute budget is measured in wall-clock
// terms against a single-threaded baseline, so overlapping independent work
// is pure savings.
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

	// Whole-stream libdeflate, whole-stream Go flate, the per-entry
	// decomposition, and the tar re-serialization share no work, so run
	// them concurrently and collect the results as they finish. Collecting
	// in this fixed order keeps the same smallest-so-far priority the
	// sequential version had.
	ldCh := make(chan []byte, 1)
	goCh := make(chan []byte, 1)
	durCh := make(chan time.Duration, 1)
	spanCh := make(chan []entrySpan, 1)
	resCh := make(chan reserialized, 1)
	go func() {
		enc, _ := libdeflateGzip(tarBytes, 12)
		ldCh <- enc
	}()
	go func() {
		enc, dur := gzipBestTimed(tarBytes)
		goCh <- enc
		durCh <- dur
	}()
	go func() {
		spanCh <- buildEntrySpans(tarBytes)
	}()
	go func() {
		res := reserializeTar(tarBytes)
		if res == nil {
			resCh <- reserialized{}
			return
		}
		ld, ok := libdeflateGzip(res, 12)
		if !ok {
			ld = nil
		}
		resCh <- reserialized{tar: res, ld: ld}
	}()

	best := inBytes
	rawLd := <-ldCh
	if rawLd != nil && len(rawLd) < len(best) {
		best = rawLd
	}
	if enc := <-goCh; len(enc) > 0 && len(enc) < len(best) {
		best = enc
	}
	res := <-resCh
	if len(res.ld) > 0 && len(res.ld) < len(best) {
		best = res.ld
	}

	if goDur := <-durCh; goDur < floorDuration {
		// The budget only affords one whole-stream zopfli pass, so give it
		// the tar whose matching libdeflate pass is smaller — a smaller
		// pre-zopfli size is the same signal that decides the per-entry
		// versus whole-stream zopfli path below.
		zdata := tarBytes
		if res.tar != nil && rawLd != nil && len(res.ld) < len(rawLd) {
			zdata = res.tar
		}
		spans := <-spanCh
		asm := assembleAndVerify(spans, tarBytes)
		if asm != nil && len(asm) < len(best) {
			// The budget only affords one zopfli path, so run the one whose
			// pre-zopfli encoding is already smaller: per-entry splitting
			// wins whenever entries want different encoders, and loses
			// whenever entries share compressible structure across their
			// boundaries.
			asm = zopfliEntries(start, spans)
			if len(asm) > 0 && len(asm) < len(best) {
				best = asm
			}
		} else if enc := tryZopfli(start, zdata); len(enc) != 0 && len(enc) < len(best) {
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

// reserialized holds a re-serialized copy of a tar stream plus the
// libdeflate pass computed over it, used to decide whether the re-serialized
// or the raw bytes are the better compression input. A zero value means the
// re-serialization was not available.
type reserialized struct {
	tar []byte
	ld  []byte
}

// tarKey is the set of header fields that must survive a round unchanged,
// mirroring the round-trip verifier's comparison — plain comparable fields
// so two entries can be compared with ==.
type tarKey struct {
	linkname           string
	uname, gname       string
	mode, uid, gid     int64
	modTime            int64
	devmajor, devminor int64
	typeflag           byte
}

func keyOf(h *tar.Header) tarKey {
	return tarKey{
		linkname: h.Linkname,
		uname:    h.Uname,
		gname:    h.Gname,
		mode:     h.Mode,
		uid:      int64(h.Uid),
		gid:      int64(h.Gid),
		modTime:  h.ModTime.Unix(),
		devmajor: h.Devmajor,
		devminor: h.Devminor,
		typeflag: h.Typeflag,
	}
}

// tarItem is one parsed entry: its header and content as the tar reader
// produced them.
type tarItem struct {
	hdr     *tar.Header
	content []byte
}

// readTarAll parses a tar stream into its entries, mirroring how the
// verifier reads one.
func readTarAll(tarBytes []byte) ([]tarItem, error) {
	tr := tar.NewReader(bytes.NewReader(tarBytes))
	var items []tarItem
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return items, nil
		}
		if err != nil {
			return nil, err
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		items = append(items, tarItem{hdr, content})
	}
}

// lastByKey collapses parsed entries to the one the verifier would see per
// name: where a name appears more than once, tar extraction takes the last.
func lastByKey(items []tarItem) map[string]tarItem {
	by := make(map[string]tarItem, len(items))
	for _, it := range items {
		by[it.hdr.Name] = it
	}
	return by
}

// reserializeTar parses a tar stream and rewrites it with Go's archive/tar
// writer, then verifies the rewrite against the verifier's own rule — every
// name's last entry must carry identical header fields and content — before
// returning it. The rewrite drops nothing the verifier tracks and normalizes
// header bytes an older or hand-rolled writer may have left in fields that
// extraction ignores, which freshly written headers then compress better.
// It returns nil if the input cannot be parsed, if any entry has a shape the
// rewrite cannot guarantee to preserve (sparse files, or content on a
// non-regular entry), or if the rewrite does not verify.
func reserializeTar(in []byte) []byte {
	items, err := readTarAll(in)
	if err != nil {
		return nil
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, it := range items {
		switch it.hdr.Typeflag {
		case tar.TypeGNUSparse:
			return nil
		}
		if it.hdr.Size > 0 && it.hdr.Typeflag != tar.TypeReg && it.hdr.Typeflag != tar.TypeRegA {
			return nil
		}
		if err := tw.WriteHeader(it.hdr); err != nil {
			return nil
		}
		if _, err := tw.Write(it.content); err != nil {
			return nil
		}
	}
	if err := tw.Close(); err != nil {
		return nil
	}
	out := buf.Bytes()

	outItems, err := readTarAll(out)
	if err != nil {
		return nil
	}
	want := lastByKey(items)
	got := lastByKey(outItems)
	if len(want) != len(got) {
		return nil
	}
	for name, a := range want {
		b, ok := got[name]
		if !ok || keyOf(a.hdr) != keyOf(b.hdr) || !bytes.Equal(a.content, b.content) {
			return nil
		}
	}
	return out
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

// buildEntrySpans partitions the decompressed tar into per-entry spans and
// encodes each span into its own gzip member with the smallest of the
// libdeflate-12 and Go-flate candidates. Spans are independent, so they are
// encoded on a pool of GOMAXPROCS workers instead of one after another —
// the pool also caps how many libdeflate compressors are alive at once,
// which keeps memory flat for archives with thousands of small entries.
// It runs concurrently with the whole-stream encodings (see run). Returns
// nil if the tar cannot be walked safely or any span has no usable
// encoding — the caller then simply has no per-entry candidate.
func buildEntrySpans(tar []byte) []entrySpan {
	spans := tarSpans(tar)
	if len(spans) == 0 {
		return nil
	}
	entries := make([]entrySpan, len(spans))
	workers := runtime.GOMAXPROCS(0)
	if workers > len(spans) {
		workers = len(spans)
	}
	idx := make(chan int, len(spans))
	for i := range spans {
		idx <- i
	}
	close(idx)

	var failed atomic.Bool
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				e := entrySpan{data: spans[i]}
				if ld, ok := libdeflateGzip(spans[i], 12); ok {
					e.ld = ld
				}
				e.gom = gzipBest(spans[i])
				switch {
				case e.ld != nil && (e.gom == nil || len(e.ld) < len(e.gom)):
					e.best = e.ld
					e.ldWon = true
				case e.gom != nil:
					e.best = e.gom
				case e.ld != nil:
					e.best = e.ld
				default:
					failed.Store(true)
					return
				}
				entries[i] = e
			}
		}()
	}
	wg.Wait()
	if failed.Load() {
		return nil
	}
	return entries
}

// assembleAndVerify concatenates the per-entry members back into one
// multi-member gzip archive and checks that it gunzips to exactly the input
// tar. The assembly is built from hand-chosen encoders, so the check guards
// against a bad span walk producing a subtly wrong archive — a failed check
// only loses the candidate, never the round. Returns nil if the spans are
// unusable or the assembly does not verify.
func assembleAndVerify(entries []entrySpan, want []byte) []byte {
	if len(entries) == 0 {
		return nil
	}
	var out []byte
	for i := range entries {
		out = append(out, entries[i].best...)
	}
	if !gunzipEquals(out, want) {
		return nil
	}
	return out
}

// zopfliEntries adds a libzopfli candidate on entries where libdeflate
// already beat Go's flate — the signal that the content rewards better
// parsers. An entry keeps its zopfli member only if it comes out smaller
// than the per-entry best. All candidate entries compress concurrently
// under the same self-imposed deadline as tryZopfli, so the wall-clock cost
// is the slowest single pass rather than the sum; a pass that has not
// finished by the deadline is abandoned along with the rest of the
// per-entry zopfli work, leaving those entries with their earlier
// encodings.
func zopfliEntries(start time.Time, entries []entrySpan) []byte {
	deadline := start.Add(budgetMultiple*floorDuration - zopfliKillMargin)

	type zopfliResult struct {
		idx int
		enc []byte
	}
	results := make(chan zopfliResult, len(entries))
	launched := 0
	for i := range entries {
		if !entries[i].ldWon {
			continue
		}
		launched++
		go func(idx int) {
			results <- zopfliResult{idx, zopfliOnce(deadline, entries[idx].data)}
		}(i)
	}
	for n := 0; n < launched; n++ {
		r := <-results
		if len(r.enc) > 0 && len(r.enc) < len(entries[r.idx].best) {
			entries[r.idx].best = r.enc
		}
	}

	var out []byte
	for i := range entries {
		out = append(out, entries[i].best...)
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
// passed in. It runs a single-iteration pass first and, when that finished
// with the deadline still far away, refines with a full 15-iteration pass —
// on the small inputs this path admits, 15 iterations beat 1 often enough
// to be the difference between beating and losing to the other encoders.
//
// The pass runs on a separate goroutine rather than inline: a
// ZopfliCompress call cannot be interrupted, but main writing its fallback
// and exiting the process stops the abandoned goroutine's work anyway.
// Returns nil if no result arrived before the deadline.
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
