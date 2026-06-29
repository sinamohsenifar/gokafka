package compress

import (
	"bytes"
	"testing"
)

// Gzip honors the compression level: a higher level produces a smaller (or
// equal) output for compressible data, and every level round-trips.
func TestGzipLevelRoundTripAndSize(t *testing.T) {
	in := bytes.Repeat([]byte("the quick brown fox jumps over the lazy dog. "), 200)
	var fastSize, bestSize int
	for _, level := range []int{0, 1, 5, 9, 99 /* clamped to 9 */} {
		out, err := Gzip(in, level)
		if err != nil {
			t.Fatalf("Gzip level %d: %v", level, err)
		}
		back, err := Gunzip(out)
		if err != nil {
			t.Fatalf("Gunzip level %d: %v", level, err)
		}
		if !bytes.Equal(back, in) {
			t.Fatalf("level %d round-trip mismatch", level)
		}
		switch level {
		case 1:
			fastSize = len(out)
		case 9:
			bestSize = len(out)
		}
	}
	if bestSize > fastSize {
		t.Errorf("level 9 (%d B) should not be larger than level 1 (%d B)", bestSize, fastSize)
	}
}

// Compress routes the level to gzip; the other codecs accept (and ignore) it.
func TestCompressAcceptsLevel(t *testing.T) {
	in := bytes.Repeat([]byte("payload"), 100)
	for _, codec := range []int8{CodecNone, CodecGzip, CodecSnappy, CodecLZ4, CodecZstd} {
		out, err := Compress(codec, 9, in)
		if err != nil {
			t.Fatalf("Compress codec %d level 9: %v", codec, err)
		}
		back, err := Decompress(codec, out)
		if err != nil {
			t.Fatalf("Decompress codec %d: %v", codec, err)
		}
		if !bytes.Equal(back, in) {
			t.Fatalf("codec %d round-trip mismatch", codec)
		}
	}
}
