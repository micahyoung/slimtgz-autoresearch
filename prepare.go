package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type datasetEntry struct {
	name string // used in per-file output and the round commit message
	path string
}

var dataset = []datasetEntry{
	{"alpine-amd64", "data/alpine-amd64.tar.gz"},
	{"alpine-arm64", "data/alpine-arm64.tar.gz"},
	{"busybox-amd64", "data/busybox-amd64.tar.gz"},
	{"busybox-arm64", "data/busybox-arm64.tar.gz"},
	{"linux001", "data/linux-0.01.tar.gz"},
	{"doomsrc", "data/doomsrc.tgz"},
}

// computeBudgetMultiple bounds how much longer train.go may take than an
// in-process gzip.BestCompression pass over the same content: a cap that
// scales with input size instead of one fixed wall-clock timeout across a
// dataset spanning 73KB to 4MB.
const computeBudgetMultiple = 100

// baselineFloor keeps the compute budget from collapsing to near-zero on
// tiny inputs, where baseline measurement noise would otherwise make the
// cap flaky rather than meaningful.
const baselineFloor = 200 * time.Millisecond

func main() {
	if err := checkIntegrity(); err != nil {
		fmt.Fprintln(os.Stderr, "integrity check failed:", err)
		os.Exit(1)
	}

	trainBin, cleanup, err := buildTrain()
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to build train.go:", err)
		os.Exit(1)
	}
	defer cleanup()

	var worstLoss, worstCompute float64
	disqualified := false

	for _, entry := range dataset {
		result, err := scoreEntry(entry, trainBin)
		if err != nil {
			fmt.Printf("%-16s DISQUALIFIED: %v\n", entry.name, err)
			disqualified = true
			continue
		}
		fmt.Printf("%-16s ratio=%.4f  compute=%.1fx  (in=%d out=%d, train=%s, baseline=%s)\n",
			entry.name, result.ratio, result.computeRatio, result.inSize, result.outSize,
			result.trainTime.Round(time.Millisecond), result.baseline.Round(time.Millisecond))

		if result.ratio > worstLoss {
			worstLoss = result.ratio
		}
		if result.computeRatio > worstCompute {
			worstCompute = result.computeRatio
		}
	}

	if disqualified {
		fmt.Println("\nROUND DISQUALIFIED: at least one dataset file failed round-trip verification or exceeded the compute budget.")
		os.Exit(1)
	}

	fmt.Printf("\nAGGREGATE LOSS %.4f\n", worstLoss)
	fmt.Printf("COMPUTE RATIO %.1fx\n", worstCompute)
}

// checkIntegrity refuses to grade against an uncommitted copy of the grader
// itself or the briefing it's graded against — the same guarantee prepare.py
// makes in screencam-autoresearch, for the same reward-hacking reason.
func checkIntegrity() error {
	cmd := exec.Command("git", "status", "--porcelain", "--", "README.md", "program.md", "prepare.go")
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("git status failed: %w", err)
	}
	if len(out) > 0 {
		return fmt.Errorf("README.md, program.md, and/or prepare.go have uncommitted changes:\n%s", out)
	}
	return nil
}

func buildTrain() (binPath string, cleanup func(), err error) {
	tmpDir, err := os.MkdirTemp("", "slimtgz-train-*")
	if err != nil {
		return "", nil, err
	}
	cleanup = func() { os.RemoveAll(tmpDir) }

	binPath = filepath.Join(tmpDir, "train")
	cmd := exec.Command("go", "build", "-o", binPath, "train.go")
	if out, err := cmd.CombinedOutput(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("%w\n%s", err, out)
	}
	return binPath, cleanup, nil
}

type scoreResult struct {
	inSize, outSize     int64
	ratio               float64
	baseline, trainTime time.Duration
	computeRatio        float64
}

func scoreEntry(entry datasetEntry, trainBin string) (scoreResult, error) {
	inBytes, err := os.ReadFile(entry.path)
	if err != nil {
		return scoreResult{}, err
	}

	tarBytes, err := gunzip(inBytes)
	if err != nil {
		return scoreResult{}, fmt.Errorf("dataset file is not valid gzip: %w", err)
	}
	baseline := measureBaselineCompress(tarBytes)
	budget := baseline * computeBudgetMultiple

	outPath := filepath.Join(os.TempDir(), entry.name+"-out.tar.gz")
	defer os.Remove(outPath)

	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	start := time.Now()
	cmd := exec.CommandContext(ctx, trainBin, entry.path, outPath)
	out, runErr := cmd.CombinedOutput()
	elapsed := time.Since(start)

	if ctx.Err() == context.DeadlineExceeded {
		return scoreResult{}, fmt.Errorf("exceeded compute budget of %s (%dx baseline %s)", budget, computeBudgetMultiple, baseline)
	}
	if runErr != nil {
		return scoreResult{}, fmt.Errorf("train run failed: %w\n%s", runErr, out)
	}

	outBytes, err := os.ReadFile(outPath)
	if err != nil {
		return scoreResult{}, fmt.Errorf("train.go did not produce an output file: %w", err)
	}

	if err := verifyRoundTrip(inBytes, outBytes); err != nil {
		return scoreResult{}, err
	}

	return scoreResult{
		inSize:       int64(len(inBytes)),
		outSize:      int64(len(outBytes)),
		ratio:        float64(len(outBytes)) / float64(len(inBytes)),
		baseline:     baseline,
		trainTime:    elapsed,
		computeRatio: float64(elapsed) / float64(baseline),
	}, nil
}

func measureBaselineCompress(tarBytes []byte) time.Duration {
	start := time.Now()
	var buf bytes.Buffer
	gw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	gw.Write(tarBytes)
	gw.Close()
	elapsed := time.Since(start)
	if elapsed < baselineFloor {
		return baselineFloor
	}
	return elapsed
}

func gunzip(data []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}

// tarMeta is every tar header field that must survive a round unchanged.
// It's a plain comparable struct (no maps, no time.Time) so two entries can
// be compared with ==.
type tarMeta struct {
	linkname           string
	uname, gname       string
	mode, uid, gid     int64
	modTime            int64
	devmajor, devminor int64
	typeflag           byte
}

type tarEntry struct {
	name    string
	meta    tarMeta
	content []byte
}

// verifyRoundTrip is the hard correctness bar every round must clear: for
// every name in the input, the output must have an entry under that name
// with every header field and every byte of content matching exactly.
// Where a name appears more than once (tar permits it; extraction takes
// the last one), comparison follows the same last-one-wins rule.
func verifyRoundTrip(inBytes, outBytes []byte) error {
	inTar, err := gunzip(inBytes)
	if err != nil {
		return fmt.Errorf("original dataset file is not valid gzip: %w", err)
	}
	inEntries, err := readTarEntries(inTar)
	if err != nil {
		return fmt.Errorf("original dataset file failed to re-parse: %w", err)
	}
	outTar, err := gunzip(outBytes)
	if err != nil {
		return fmt.Errorf("output is not valid gzip: %w", err)
	}
	outEntries, err := readTarEntries(outTar)
	if err != nil {
		return fmt.Errorf("output is not a valid tar: %w", err)
	}

	inByName := lastByName(inEntries)
	outByName := lastByName(outEntries)

	if len(inByName) != len(outByName) {
		return fmt.Errorf("entry count mismatch: in=%d out=%d", len(inByName), len(outByName))
	}
	for name, a := range inByName {
		b, ok := outByName[name]
		if !ok {
			return fmt.Errorf("entry %q missing from output", name)
		}
		if a.meta != b.meta {
			return fmt.Errorf("entry %q metadata mismatch: %+v vs %+v", name, a.meta, b.meta)
		}
		if !bytes.Equal(a.content, b.content) {
			return fmt.Errorf("entry %q content mismatch", name)
		}
	}
	return nil
}

func lastByName(entries []tarEntry) map[string]tarEntry {
	byName := make(map[string]tarEntry, len(entries))
	for _, e := range entries {
		byName[e.name] = e
	}
	return byName
}

func readTarEntries(tarBytes []byte) ([]tarEntry, error) {
	tr := tar.NewReader(bytes.NewReader(tarBytes))
	var entries []tarEntry
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		content, err := io.ReadAll(tr)
		if err != nil {
			return nil, err
		}
		entries = append(entries, tarEntry{
			name: hdr.Name,
			meta: tarMeta{
				linkname: hdr.Linkname,
				uname:    hdr.Uname,
				gname:    hdr.Gname,
				mode:     hdr.Mode,
				uid:      int64(hdr.Uid),
				gid:      int64(hdr.Gid),
				modTime:  hdr.ModTime.Unix(),
				devmajor: hdr.Devmajor,
				devminor: hdr.Devminor,
				typeflag: hdr.Typeflag,
			},
			content: content,
		})
	}
	return entries, nil
}
