package compress

import (
	"github.com/sinamohsenifar/gokafka/internal/compress/zstd"
	"github.com/sinamohsenifar/gokafka/internal/limits"
)

// IsZstdFrame reports whether data begins with a ZSTD frame magic.
func IsZstdFrame(data []byte) bool {
	return zstd.IsFrame(data)
}

// ZstdEncode compresses data using standard ZSTD frames (Kafka codec 4).
func ZstdEncode(in []byte) ([]byte, error) {
	return zstd.Encode(in)
}

// ZstdDecode decompresses standard ZSTD frames.
func ZstdDecode(in []byte) ([]byte, error) {
	return zstd.Decode(in, limits.MaxDecompressedBytes())
}
