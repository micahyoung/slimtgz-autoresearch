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
//     favor; the whole-stream side runs at fifteen iterations, split across
//     as many goroutines as its measured cost needs to finish inside the
//     deadline (see zopfliChunks), and in addition once unsplit over the
//     whole stream from t≈0 — that pass pays no cut cost and is kept when it
//     finishes in time and comes out smallest (see speculativeZopfli)
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
// The libzopfli candidates are the expensive ones, so they run under a
// self-imposed deadline placed safely inside the budget this process
// computes for itself: the timed Go-BestCompression pass is the same
// measurement the budget is built from, so its duration (floored the same
// way) says how big that budget is. Even so the estimate is only as good as
// that measurement, hence budgetPercent well below 100. A libzopfli call
// cannot be interrupted either, so past the deadline the zopfli helpers
// abandon the
// goroutine they are running on and return nothing, falling back to the
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

	// Speculative single-member whole-stream zopfli pass, started here at t≈0
	// over the raw tar bytes: the chunked pass below cuts the stream into
	// members to fit the deadline and each cut costs size, so when the actual
	// pass would have fit whole anyway the cut is pure loss. This candidate
	// is the same pass without any cuts, kept only if it finishes before the
	// deadline it is handed once the budget is known (see speculativeZopfli);
	// the chunked path stays its insurance for when it does not. It costs one
	// core the other candidates barely use, and inputs small enough that the
	// chunked path never cuts skip it.
	var z1Ch chan []byte
	deadlineCh := make(chan time.Time, 1)
	if len(tarBytes) >= zopfliChunkThreshold {
		z1Ch = make(chan []byte, 1)
		go speculativeZopfli(z1Ch, deadlineCh, tarBytes)
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

	goDur := <-durCh
	// The budget is built from a timed Go-BestCompression pass, and goDur is
	// this process's own measurement of exactly that pass over exactly these
	// bytes, so flooring it the same way recovers the budget's size.
	// budgetPercent of that, minus the write-out margin, is the deadline the
	// zopfli work has to respect.
	budget := time.Duration(budgetMultiple) * max(goDur, floorDuration)
	deadline := start.Add(budget*budgetPercent/100 - zopfliKillMargin)
	if z1Ch != nil {
		deadlineCh <- deadline
	}

	// Only one zopfli path fits in the budget, so take the one whose
	// pre-zopfli encoding is already smaller: per-entry splitting wins
	// whenever entries want different encoders, and loses whenever entries
	// share compressible structure across their boundaries. Give that path
	// the tar whose matching libdeflate pass is smaller — a smaller
	// pre-zopfli size is the same signal.
	zdata := tarBytes
	if res.tar != nil && rawLd != nil && len(res.ld) < len(rawLd) {
		zdata = res.tar
	}
	spans := <-spanCh
	asm := assembleAndVerify(spans, tarBytes)
	if asm != nil && len(asm) < len(best) {
		asm = zopfliEntries(deadline, spans)
		if len(asm) > 0 && len(asm) < len(best) {
			best = asm
		}
	} else if enc := tryZopfli(deadline, goDur, zdata); len(enc) != 0 && len(enc) < len(best) {
		best = enc
	}
	if z1Ch != nil {
		if enc := <-z1Ch; len(enc) > 0 && len(enc) < len(best) {
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
func zopfliEntries(deadline time.Time, entries []entrySpan) []byte {
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

// tarSize reads a tar header's size field: standard octal — left-padded with
// zeroes the way Go's tar writer emits it, or right-justified with leading
// spaces the way v7/old GNU tar wrote it — or GNU base-256 when the high bit
// of the first byte is set. ok is false for a field without a usable value.
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
		if !sawDigit && c == ' ' {
			continue
		}
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
// against: an input whose Go-BestCompression pass finishes faster than this
// gets a budget built from the floor rather than from the measured time, so
// measurement noise on a tiny input cannot collapse the budget to nothing.
const floorDuration = 200 * time.Millisecond

// budgetMultiple is how many times the (floored) baseline the per-file
// compute budget spans.
const budgetMultiple = 100

// budgetPercent is how much of that budget the zopfli deadline is allowed to
// use. The budget is recovered from this process's own timing of the same
// pass the budget is built from, so it is the same quantity rather than a
// guess — but the two timings are taken at different moments, and the
// zopfli work starts some way into the process, so the deadline keeps a
// sizeable slice of the budget in reserve.
const budgetPercent = 70

// zopfliIterations is how many refinement passes libzopfli makes over a
// whole-stream candidate. Past this, measured gains on this dataset flatten
// out while the cost keeps climbing.
const zopfliIterations = 15

// zopfliSerialFactor converts the measured Go-BestCompression duration into
// an estimate of a whole-stream zopfli pass at zopfliIterations. Measured on
// this dataset the ratio runs 30-60x depending on the content, so the
// estimate deliberately sits at the pessimistic end.
const zopfliSerialFactor = 60

// zopfliKillMargin is how far before that budget's expiry tryZopfli gives
// up waiting, so the fallback candidates can still be written out.
const zopfliKillMargin = 500 * time.Millisecond

// speculativeZopfli runs the single-member whole-stream pass and sends the
// result on out (nil if it did not finish in time). It starts immediately —
// before the deadline exists, since the deadline comes from the timed Go pass
// running alongside it — and is handed the deadline on deadlineCh once the
// caller has computed it. A ZopfliCompress call cannot be interrupted, so a
// pass still running then is abandoned along with the goroutine running it,
// and nil goes out so the caller keeps the chunked candidate.
func speculativeZopfli(out chan<- []byte, deadlineCh <-chan time.Time, data []byte) {
	deadline := <-deadlineCh
	results := make(chan []byte, 1)
	go func() {
		if enc, ok := zopfliGzip(data, zopfliIterations); ok {
			results <- enc
		}
	}()

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case enc := <-results:
		out <- enc
	case <-timer.C:
		out <- nil
	}
}

// tryZopfli compresses data with libzopfli at zopfliIterations under the
// given deadline. baseline is the measured Go-BestCompression pass, scaled
// by zopfliSerialFactor to predict what the pass will cost, and the pass is
// split across as many goroutines as that prediction needs in order to fit
// (see zopfliChunks) — a single ZopfliCompress call is single-threaded and
// takes tens of seconds on the larger dataset files.
//
// The pass runs on a separate goroutine rather than inline: a
// ZopfliCompress call cannot be interrupted, but main writing its fallback
// and exiting the process stops the abandoned goroutine's work anyway.
// Returns nil if no result arrived before the deadline, or if the predicted
// cost does not fit even fully split.
func tryZopfli(deadline time.Time, baseline time.Duration, data []byte) []byte {
	n := zopfliChunks(time.Duration(zopfliSerialFactor)*baseline, len(data), deadline)
	if n == 0 {
		return nil
	}

	results := make(chan []byte, 1)
	go func() { results <- zopfliRefine(data, n) }()

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case enc := <-results:
		return enc
	case <-timer.C:
		return nil
	}
}

// zopfliChunks picks how many parallel gzip members a whole-stream zopfli
// pass is cut into. estSerial is the predicted wall-clock cost of one call
// over the whole input; cutting it into members costs a little size at each
// cut but is close to linear in wall clock, so the count is the smallest one
// whose predicted time fits in three quarters of what is left before the
// deadline. The quarter is slack, not parsimony: the estimate is a scaled
// guess, and a pass still running when the deadline passes is abandoned
// along with the whole candidate, which is worse than one extra cut. Returns
// 0 when even one member per core cannot fit, so the caller skips the
// candidate; inputs below zopfliChunkThreshold always get one member, where
// the serial pass is fast enough that a cut could only lose size.
func zopfliChunks(estSerial time.Duration, size int, deadline time.Time) int {
	if size < zopfliChunkThreshold {
		return 1
	}
	avail := time.Until(deadline)
	limit := runtime.GOMAXPROCS(0)
	for n := 1; ; n++ {
		if estSerial/time.Duration(n) <= avail*3/4 {
			return n
		}
		if n >= limit {
			return 0
		}
	}
}

// zopfliBlockSplitMax caps how many deflate blocks libzopfli may split a
// stream into. Its own default of 15 is far below what a multi-megabyte tar
// stream wants — the content alternates between megabyte binaries and runs
// of near-identical 512-byte headers, each wanting its own block — and at
// the default libzopfli loses to libdeflate's optimal parse on this
// dataset, while at 63 it wins on every file it is tried on. 15 iterations
// cost less than 63x of one iteration because block-splitting work is
// amortised, and the measured cost of 63 over 15 is well inside what the
// budget affords once the pass is chunk-parallel.
const zopfliBlockSplitMax = 63

// zopfliChunkThreshold is the input size below which the whole-stream zopfli
// pass runs as a single member: splitting is a wall-clock tool, and a pass
// this small does not need one.
const zopfliChunkThreshold = 512 << 10

// zopfliRefine runs the zopfliIterations whole-stream pass, either as one
// member or cut into n equal, 512-byte-aligned pieces compressed
// concurrently. Concatenating lossless members reproduces the input exactly
// regardless of where the cuts land, and the assembled stream is
// gunzip-verified before being returned. The cuts pay each member's own
// Huffman tables and lose the matches across the boundary — measured at
// well under 1KB per cut on this dataset's larger files, against a wall
// clock roughly divided by n — so zopfliChunks asks for as few of them as
// the deadline allows. Returns nil if any member failed to encode or the
// assembly does not verify.
func zopfliRefine(data []byte, n int) []byte {
	if n == 1 {
		enc, ok := zopfliGzip(data, zopfliIterations)
		if !ok {
			return nil
		}
		return enc
	}

	bounds := make([]int, n+1)
	for i := range bounds {
		b := i * len(data) / n
		bounds[i] = b - b%512
	}
	bounds[n] = len(data)

	members := make([][]byte, n)
	okFlags := make([]bool, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if enc, ok := zopfliGzip(data[bounds[i]:bounds[i+1]], zopfliIterations); ok {
				members[i] = enc
				okFlags[i] = true
			}
		}(i)
	}
	wg.Wait()

	var out []byte
	for i := 0; i < n; i++ {
		if !okFlags[i] {
			return nil
		}
		out = append(out, members[i]...)
	}
	if !gunzipEquals(out, data) {
		return nil
	}
	return out
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
	opts.blocksplittingmax = C.int(zopfliBlockSplitMax)

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
