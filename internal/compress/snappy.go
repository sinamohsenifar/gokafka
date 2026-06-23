package compress

import (
	"errors"
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/limits"
)

var errSnappyCorrupt = errors.New("snappy: corrupt input")

// SnappyEncode compresses using Google Snappy block format (Kafka codec 2).
func SnappyEncode(src []byte) ([]byte, error) {
	dst := encodeSnappyVarint(uint64(len(src)))
	for i := 0; i < len(src); {
		n := len(src) - i
		if n > 60 {
			n = 60
		}
		dst = appendSnappyLiteral(dst, src[i:i+n])
		i += n
	}
	return dst, nil
}

// SnappyDecode decompresses Google Snappy block format.
func SnappyDecode(src []byte) ([]byte, error) {
	uncompressed, n := decodeSnappyVarint(src)
	if n <= 0 {
		return nil, errSnappyCorrupt
	}
	if uncompressed > uint64(limits.MaxDecompressedBytes) {
		return nil, fmt.Errorf("snappy: declared size %d exceeds limit %d", uncompressed, limits.MaxDecompressedBytes)
	}
	src = src[n:]
	out := make([]byte, 0, int(uncompressed))
	for len(out) < int(uncompressed) {
		if len(src) == 0 {
			return nil, errSnappyCorrupt
		}
		tag := src[0]
		src = src[1:]
		switch tag & 0x3 {
		case 0:
			litLen, rest, err := snappyLiteralLen(tag, src)
			if err != nil {
				return nil, err
			}
			src = rest
			if len(src) < litLen {
				return nil, errSnappyCorrupt
			}
			out = append(out, src[:litLen]...)
			src = src[litLen:]
		case 1:
			if len(src) < 2 {
				return nil, errSnappyCorrupt
			}
			length := int(tag>>2&0x7) + 4
			offset := int(src[0]) | int(src[1])<<8 | int(tag&0xe0)<<3
			src = src[2:]
			if offset == 0 || offset > len(out) {
				return nil, errSnappyCorrupt
			}
			out = snappyCopy(out, offset, length)
		case 2:
			if len(src) < 2 {
				return nil, errSnappyCorrupt
			}
			length := int(tag>>2&0x7) + 9
			offset := int(src[0]) | int(src[1])<<8
			src = src[2:]
			if offset == 0 || offset > len(out) {
				return nil, errSnappyCorrupt
			}
			out = snappyCopy(out, offset, length)
		case 3:
			if len(src) < 4 {
				return nil, errSnappyCorrupt
			}
			length := int(tag>>2&0x7) + 17
			offset := int(src[0]) | int(src[1])<<8 | int(src[2])<<16 | int(src[3])<<24
			src = src[4:]
			if offset == 0 || offset > len(out) {
				return nil, errSnappyCorrupt
			}
			out = snappyCopy(out, offset, length)
		}
	}
	if len(out) != int(uncompressed) {
		return nil, errSnappyCorrupt
	}
	return out, nil
}

func snappyCopy(out []byte, offset, length int) []byte {
	for i := 0; i < length; i++ {
		out = append(out, out[len(out)-offset])
	}
	return out
}

func appendSnappyLiteral(dst, lit []byte) []byte {
	for len(lit) > 0 {
		n := len(lit)
		if n > 60 {
			n = 60
		}
		dst = append(dst, byte((n-1)<<2))
		dst = append(dst, lit[:n]...)
		lit = lit[n:]
	}
	return dst
}

func encodeSnappyVarint(v uint64) []byte {
	dst := make([]byte, 0, 10)
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}

func decodeSnappyVarint(src []byte) (uint64, int) {
	var x uint64
	for i, b := range src {
		if i >= 10 {
			return 0, 0
		}
		x |= uint64(b&0x7f) << (7 * i)
		if b < 0x80 {
			return x, i + 1
		}
	}
	return 0, 0
}

func snappyLiteralLen(tag byte, src []byte) (int, []byte, error) {
	if tag < 0xf0 {
		return int(tag>>2) + 1, src, nil
	}
	switch tag {
	case 0xf0:
		if len(src) < 1 {
			return 0, src, errSnappyCorrupt
		}
		return int(src[0]) + 1, src[1:], nil
	case 0xf1:
		if len(src) < 2 {
			return 0, src, errSnappyCorrupt
		}
		return (int(src[0]) | int(src[1])<<8) + 1, src[2:], nil
	case 0xf2:
		if len(src) < 3 {
			return 0, src, errSnappyCorrupt
		}
		return (int(src[0]) | int(src[1])<<8 | int(src[2])<<16) + 1, src[3:], nil
	case 0xf3:
		if len(src) < 4 {
			return 0, src, errSnappyCorrupt
		}
		return (int(src[0]) | int(src[1])<<8 | int(src[2])<<16 | int(src[3])<<24) + 1, src[4:], nil
	default:
		return 0, src, errSnappyCorrupt
	}
}
