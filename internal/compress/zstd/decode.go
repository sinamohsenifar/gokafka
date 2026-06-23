package zstd

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// ErrCorrupt indicates invalid or truncated ZSTD input.
var ErrCorrupt = errors.New("zstd: corrupt input")

// Magic is the little-endian ZSTD frame magic (RFC 8878).
const Magic uint32 = 0xFD2FB528

// IsFrame reports whether data begins with a ZSTD frame magic.
func IsFrame(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return uint32(data[0])|uint32(data[1])<<8|uint32(data[2])<<16|uint32(data[3])<<24 == Magic
}

// Decode decompresses a standard ZSTD frame (Kafka codec 4 payload).
func Decode(src []byte, maxSize int) ([]byte, error) {
	if len(src) == 0 {
		return nil, ErrCorrupt
	}
	if !IsFrame(src) {
		return nil, ErrCorrupt
	}
	r := NewReader(bytes.NewReader(src))
	limited := io.LimitReader(r, int64(maxSize)+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		var ze *zstdError
		if errors.As(err, &ze) {
			return nil, fmt.Errorf("%w: %v", ErrCorrupt, ze.Unwrap())
		}
		return nil, err
	}
	if len(out) > maxSize {
		return nil, fmt.Errorf("zstd: decompressed size exceeds limit %d", maxSize)
	}
	return out, nil
}
