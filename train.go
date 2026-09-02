package main

/*
#cgo LDFLAGS: -ldeflate
#include <libdeflate.h>
#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
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
//   - when the external zopfli binary is available and there is provably
//     enough time for a full pass: zopfli at 1 iteration, then again at 15
//     iterations if the first pass finished with time to spare
//
// The last-named candidate only runs on inputs that compress well enough in
// Go's flate that the time budget is governed by the fixed floor rather than
// by a measured time (see flooredBudget). For those inputs the budget is a
// known constant, so zopfli can be given a hard self-imposed kill deadline
// below it and never risks blowing the budget: a zopfli pass that overruns
// is killed and the encoder simply falls back to the other candidates.
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

	_, lookErr := exec.LookPath("zopfli")
	if goBestDur < floorDuration && lookErr == nil {
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

// zopfliKillMargin is how far before that budget's expiry zopfli passes are
// killed, so the fallback candidates can still be written out.
const zopfliKillMargin = 500 * time.Millisecond

// tryZopfli runs the external zopfli compressor over data, under a deadline
// derived from the (constant, floored) compute budget, measured from the
// process start passed in. It first runs a single-iteration pass, which
// already finds nearly all of the gain a zopfli run can offer; it only
// follows up with a full fifteen-iteration pass when that first pass came
// back with the whole budget's slack to spare. Returns nil when zopfli is
// unavailable, fails, or hits the deadline.
func tryZopfli(start time.Time, data []byte) []byte {
	budget := budgetMultiple * floorDuration
	ctx, cancel := context.WithDeadline(context.Background(), start.Add(budget-zopfliKillMargin))
	defer cancel()

	tmp, err := os.CreateTemp("", "slimtargz-")
	if err != nil {
		return nil
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return nil
	}
	if err := tmp.Close(); err != nil {
		return nil
	}

	first, ok := zopfliPass(ctx, "--i1", tmp.Name())
	if !ok {
		return nil
	}
	if time.Since(start) > budget/4 {
		return first
	}
	if second, ok := zopfliPass(ctx, "--i15", tmp.Name()); ok && len(second) < len(first) {
		return second
	}
	return first
}

// zopfliPass runs one zopfli compression pass with the given iteration flag
// over the file at path, returning its gzip output.
func zopfliPass(ctx context.Context, iterFlag, path string) ([]byte, bool) {
	cmd := exec.CommandContext(ctx, "zopfli", iterFlag, "-c", path)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return nil, false
	}
	if out.Len() == 0 {
		return nil, false
	}
	return out.Bytes(), true
}

func gunzipAll(data []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}
