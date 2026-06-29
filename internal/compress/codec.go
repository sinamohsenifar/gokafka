package compress

import "fmt"

// Kafka compression codec ids (lower 3 bits of record batch attributes).
const (
	CodecNone   int8 = 0
	CodecGzip   int8 = 1
	CodecSnappy int8 = 2
	CodecLZ4    int8 = 3
	CodecZstd   int8 = 4
)

// Compress compresses data using the Kafka codec id. level is the requested
// compression level (KIP-390); it is honored for gzip and ignored by the
// fixed-strategy pure-Go snappy/lz4/zstd encoders. level <= 0 means default.
func Compress(codec int8, level int, in []byte) ([]byte, error) {
	switch codec {
	case CodecNone:
		return append([]byte(nil), in...), nil
	case CodecGzip:
		return Gzip(in, level)
	case CodecSnappy:
		return SnappyEncode(in)
	case CodecLZ4:
		return LZ4Encode(in)
	case CodecZstd:
		return ZstdEncode(in)
	default:
		return nil, fmt.Errorf("compress: unknown codec %d", codec)
	}
}

// Decompress decompresses data using the Kafka codec id.
func Decompress(codec int8, in []byte) ([]byte, error) {
	switch codec {
	case CodecNone:
		return append([]byte(nil), in...), nil
	case CodecGzip:
		return Gunzip(in)
	case CodecSnappy:
		return SnappyDecode(in)
	case CodecLZ4:
		return LZ4Decode(in)
	case CodecZstd:
		return ZstdDecode(in)
	default:
		return nil, fmt.Errorf("compress: unknown codec %d", codec)
	}
}
