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
//
// The last-named candidate only runs on inputs that compress well enough in
// Go's flate that the compute budget is governed by the fixed floor rather
// than by a measured time (see floorDuration). For those inputs the budget
// is a known constant, so zopfli can be given a self-imposed deadline below
// it and never risks blowing the budget: a libzopfli call cannot be
// interrupted, so past the deadline tryZopfli abandons the goroutine it is
// running and returns nothing, and the encoder simply falls back to the
// other candidates.
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
		if enc := tryZopfli(start, tarBytes); len(enc) != 0 && len(enc) < len(best) {
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
	var buf bytes.Buffer
	gw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return nil, time.Since(start)
	}
	if _, err := gw.Write(data); err != nil {
		return nil, time.Since(start)
	}
	if err := gw.Close(); err != nil {
		return nil, time.Since(start)
	}
	return buf.Bytes(), time.Since(start)
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
