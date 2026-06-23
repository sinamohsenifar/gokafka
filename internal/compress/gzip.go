package compress

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/sinamohsenifar/gokafka/internal/limits"
)

// Gzip compresses data using stdlib gzip (Kafka compression type 1).
func Gzip(in []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(in); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Gunzip decompresses gzip data.
func Gunzip(in []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	limited := io.LimitReader(r, int64(limits.MaxDecompressedBytes)+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(out) > limits.MaxDecompressedBytes {
		return nil, fmt.Errorf("compress: gzip decompressed size exceeds limit %d", limits.MaxDecompressedBytes)
	}
	return out, nil
}
