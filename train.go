package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
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
// the input once, re-encodes the tar bytes with gzip at the best available
// deflate level, and writes whichever of the two (fresh encoding vs. the
// original bytes) is smaller — Go's flate is weaker than the zlib-class
// compressors that produced most real .tar.gz files, so a blind
// re-encode sometimes inflates them.
func run(inPath, outPath string) error {
	inBytes, err := os.ReadFile(inPath)
	if err != nil {
		return err
	}

	tarBytes, err := gunzipAll(inBytes)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	gw, err := gzip.NewWriterLevel(&buf, gzip.BestCompression)
	if err != nil {
		return err
	}
	if _, err := gw.Write(tarBytes); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}

	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	var best []byte
	if buf.Len() < len(inBytes) {
		best = buf.Bytes()
	} else {
		best = inBytes
	}
	_, err = out.Write(best)
	return err
}

func gunzipAll(data []byte) ([]byte, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	return io.ReadAll(gr)
}
