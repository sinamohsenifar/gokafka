package compress_test

import (
	"bytes"
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/compress"
)

func TestGzipRoundTrip(t *testing.T) {
	in := bytes.Repeat([]byte("kafka"), 100)
	out, err := compress.Compress(compress.CodecGzip, in)
	if err != nil {
		t.Fatal(err)
	}
	back, err := compress.Decompress(compress.CodecGzip, out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(in, back) {
		t.Fatal("gzip roundtrip mismatch")
	}
}

func TestSnappyIntegrationPayload(t *testing.T) {
	in := bytes.Repeat([]byte("compressed-2-"), 32)
	out, err := compress.Compress(compress.CodecSnappy, in)
	if err != nil {
		t.Fatal(err)
	}
	back, err := compress.Decompress(compress.CodecSnappy, out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(in, back) {
		t.Fatal("snappy integration payload roundtrip mismatch")
	}
}

func TestSnappyRoundTrip(t *testing.T) {
	in := []byte("hello snappy compression for kafka records")
	out, err := compress.Compress(compress.CodecSnappy, in)
	if err != nil {
		t.Fatal(err)
	}
	back, err := compress.Decompress(compress.CodecSnappy, out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(in, back) {
		t.Fatal("snappy roundtrip mismatch")
	}
}

func TestLZ4RoundTrip(t *testing.T) {
	in := bytes.Repeat([]byte("lz4"), 200)
	out, err := compress.Compress(compress.CodecLZ4, in)
	if err != nil {
		t.Fatal(err)
	}
	back, err := compress.Decompress(compress.CodecLZ4, out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(in, back) {
		t.Fatal("lz4 roundtrip mismatch")
	}
}

func TestZstdRoundTrip(t *testing.T) {
	in := bytes.Repeat([]byte("kafka-zstd-"), 48)
	out, err := compress.Compress(compress.CodecZstd, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) >= len(in) {
		t.Fatalf("expected compression, got %d >= %d", len(out), len(in))
	}
	back, err := compress.Decompress(compress.CodecZstd, out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(in, back) {
		t.Fatal("zstd roundtrip mismatch")
	}
}
