package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"testing"
	"time"
)

var gzipHi = func() []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	for i := range 10_000 {
		w.Write([]byte(`{"id":` + fmt.Sprint(i) + `}` + "\n"))
	}
	w.Close()
	return b.Bytes()
}()

type TinyGzipReader struct{ pos int }

func (t *TinyGzipReader) Read(p []byte) (int, error) {
	if t.pos >= len(gzipHi) {
		return 0, io.EOF
	}
	n := copy(p, gzipHi[t.pos:])
	t.pos += n
	return n, nil
}

func parse(jsonBytes []byte) int {
	x := 0
	for i := range 100_000 {
		// mix in some data so compiler can't remove loop
		x = (x*131 + int(jsonBytes[0])) ^ i
	}
	return x
}

func TestStreamReader(t *testing.T) {
	numThreads := []int{1, 2, 4, 8, 16, 32, 64}
	for _, N := range numThreads {
		start := time.Now()
		out := streamReader(&TinyGzipReader{}, parse, N)
		count := 0
		for range out {
			count++
		}
		elapsed := time.Since(start)
		fmt.Printf("Workers=%2d  Time=%v  Records=%d\n", N, elapsed, count)

		if count == 0 {
			t.Fatalf("streamReader produced no output for N=%d", N)
		}
	}
}
