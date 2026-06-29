package compress

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/limits"
)

var errLZ4 = errors.New("lz4: corrupt or unsupported block")

// LZ4Encode compresses using Kafka LZ4 framing (codec 3).
func LZ4Encode(src []byte) ([]byte, error) {
	compressed := lz4CompressBlock(src)
	out := make([]byte, 8+len(compressed))
	binary.BigEndian.PutUint32(out[0:4], uint32(len(compressed)))
	binary.BigEndian.PutUint32(out[4:8], uint32(len(src)))
	copy(out[8:], compressed)
	return out, nil
}

// LZ4Decode decompresses Kafka LZ4 framing.
func LZ4Decode(src []byte) ([]byte, error) {
	if len(src) < 8 {
		return nil, errLZ4
	}
	compLen := int(binary.BigEndian.Uint32(src[0:4]))
	uncompLen := int(binary.BigEndian.Uint32(src[4:8]))
	if uncompLen > limits.MaxDecompressedBytes() {
		return nil, fmt.Errorf("lz4: declared size %d exceeds limit %d", uncompLen, limits.MaxDecompressedBytes())
	}
	if len(src) < 8+compLen {
		return nil, errLZ4
	}
	return lz4DecompressBlock(src[8:8+compLen], uncompLen)
}

func lz4CompressBlock(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	out := make([]byte, 0, len(src))
	i := 0
	for i < len(src) {
		// Greedy match search (min match len 4).
		bestOff, bestLen := 0, 0
		start := i - 65535
		if start < 0 {
			start = 0
		}
		for off := i - 1; off >= start; off-- {
			maxLen := len(src) - i
			if maxLen > 65535 {
				maxLen = 65535
			}
			l := 0
			for l < maxLen && src[off+l] == src[i+l] {
				l++
			}
			if l >= 4 && l > bestLen {
				bestOff = i - off
				bestLen = l
			}
		}

		litStart := i
		if bestLen >= 4 {
			i += bestLen
		} else {
			i++
			bestLen = 0
		}
		litLen := i - litStart
		if bestLen == 0 {
			// trailing literals only
			litLen = len(src) - litStart
			i = len(src)
		}
		writeLiteralRun(&out, src[litStart:litStart+litLen])
		if bestLen >= 4 {
			writeMatch(&out, bestOff, bestLen)
		}
	}
	return out
}

func writeLiteralRun(out *[]byte, lit []byte) {
	remaining, off := len(lit), 0
	for remaining > 0 {
		n := remaining
		if n > 65535 {
			n = 65535
		}
		if n >= 15 {
			*out = append(*out, 0xf0)
			x := n - 15
			for x >= 255 {
				*out = append(*out, 255)
				x -= 255
			}
			*out = append(*out, byte(x))
		} else {
			*out = append(*out, byte(n<<4))
		}
		*out = append(*out, lit[off:off+n]...)
		off += n
		remaining -= n
	}
}

func writeMatch(out *[]byte, offset, length int) {
	matchLen := length
	for matchLen > 0 {
		chunk := matchLen
		if chunk > 15+4 {
			chunk = 15 + 4
		}
		token := byte(0)
		if chunk >= 4+15 {
			token = 0x0f
			extra := chunk - 4 - 15
			*out = append(*out, token)
			*out = append(*out, byte(offset&0xff), byte(offset>>8))
			for extra >= 255 {
				*out = append(*out, 255)
				extra -= 255
			}
			*out = append(*out, byte(extra))
		} else {
			token = byte((chunk - 4) << 0)
			*out = append(*out, token)
			*out = append(*out, byte(offset&0xff), byte(offset>>8))
		}
		matchLen -= chunk
	}
}

func lz4DecompressBlock(src []byte, uncompLen int) ([]byte, error) {
	out := make([]byte, 0, uncompLen)
	i := 0
	for len(out) < uncompLen {
		if i >= len(src) {
			return nil, errLZ4
		}
		token := src[i]
		i++
		litLen := int(token >> 4)
		if litLen == 15 {
			for {
				if i >= len(src) {
					return nil, errLZ4
				}
				b := int(src[i])
				i++
				litLen += b
				if b != 255 {
					break
				}
			}
		}
		if i+litLen > len(src) {
			return nil, errLZ4
		}
		out = append(out, src[i:i+litLen]...)
		i += litLen
		if len(out) >= uncompLen {
			break
		}
		if i+2 > len(src) {
			return nil, errLZ4
		}
		offset := int(src[i]) | int(src[i+1])<<8
		i += 2
		if offset == 0 || offset > len(out) {
			return nil, errLZ4
		}
		matchLen := int(token&0x0f) + 4
		if token&0x0f == 15 {
			for {
				if i >= len(src) {
					return nil, errLZ4
				}
				b := int(src[i])
				i++
				matchLen += b
				if b != 255 {
					break
				}
			}
		}
		for j := 0; j < matchLen; j++ {
			out = append(out, out[len(out)-offset])
		}
	}
	if len(out) != uncompLen {
		return nil, errLZ4
	}
	return out, nil
}
