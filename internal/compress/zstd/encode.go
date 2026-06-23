package zstd

import (
	"encoding/binary"
)

const maxBlockSize = 128 << 10

// Encode compresses src into a standard single-segment ZSTD frame.
func Encode(src []byte) ([]byte, error) {
	initPredefinedEncoders()
	if len(src) == 0 {
		return encodeEmptyFrame(), nil
	}
	if len(src) > maxBlockSize {
		return encodeLarge(src)
	}
	block, ok := encodeCompressedBlock(src)
	if !ok || len(block) >= len(src) {
		return encodeRawFrame(src), nil
	}
	return appendFrame(nil, src, block), nil
}

func encodeEmptyFrame() []byte {
	// Single-segment empty frame with zero-size last raw block.
	return []byte{0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x00, 0x01, 0x00, 0x00}
}

func encodeRawFrame(src []byte) []byte {
	block := make([]byte, 3+len(src))
	hdr := uint32(1) | (0 << 1) | (uint32(len(src)) << 3) // last raw block
	block[0] = byte(hdr)
	block[1] = byte(hdr >> 8)
	block[2] = byte(hdr >> 16)
	copy(block[3:], src)
	return appendFrame(nil, src, block)
}

func encodeLarge(src []byte) ([]byte, error) {
	out := make([]byte, 0, len(src))
	for len(src) > 0 {
		chunk := src
		if len(chunk) > maxBlockSize {
			chunk = chunk[:maxBlockSize]
		}
		src = src[len(chunk):]
		block, ok := encodeCompressedBlock(chunk)
		last := len(src) == 0
		if !ok || len(block) >= len(chunk) {
			block = encodeRawBlock(chunk, last)
		} else if !last {
			block = setBlockLast(block, false)
		}
		out = appendFrame(out, chunk, block)
	}
	return out, nil
}

func appendFrame(dst, src, block []byte) []byte {
	frame := encodeFrameHeader(len(src))
	dst = append(dst, frame...)
	return append(dst, block...)
}

func encodeFrameHeader(contentSize int) []byte {
	var hdr [14]byte
	hdr[0] = 0x28
	hdr[1] = 0xb5
	hdr[2] = 0x2f
	hdr[3] = 0xfd

	switch {
	case contentSize < 256:
		hdr[4] = 0x20 // single segment, 1-byte FCS
		hdr[5] = byte(contentSize)
		return hdr[:6]
	case contentSize < 256+65535:
		hdr[4] = 0x60 // single segment, 2-byte FCS
		binary.LittleEndian.PutUint16(hdr[5:7], uint16(contentSize-256))
		return hdr[:7]
	case contentSize < 1<<32:
		hdr[4] = 0xa0 // single segment, 4-byte FCS
		binary.LittleEndian.PutUint32(hdr[5:9], uint32(contentSize))
		return hdr[:9]
	default:
		hdr[4] = 0xe0 // single segment, 8-byte FCS
		binary.LittleEndian.PutUint64(hdr[5:13], uint64(contentSize))
		return hdr[:13]
	}
}

func encodeRawBlock(src []byte, last bool) []byte {
	block := make([]byte, 3+len(src))
	hdr := uint32(len(src)) << 3
	if last {
		hdr |= 1
	}
	block[0] = byte(hdr)
	block[1] = byte(hdr >> 8)
	block[2] = byte(hdr >> 16)
	copy(block[3:], src)
	return block
}

func setBlockLast(block []byte, last bool) []byte {
	out := append([]byte(nil), block...)
	if last {
		out[0] |= 1
	} else {
		out[0] &^= 1
	}
	return out
}

func encodeCompressedBlock(src []byte) ([]byte, bool) {
	literals, sequences := buildSequences(src)
	if len(sequences) == 0 {
		return nil, false
	}
	blockBody := make([]byte, 0, len(literals)+len(sequences)*8+64)
	blockBody = appendRawLiteralsHeader(blockBody, len(literals))
	blockBody = append(blockBody, literals...)
	blockBody = encodeSequences(blockBody, sequences)

	hdr := uint32(1) | (2 << 1) | (uint32(len(blockBody)) << 3) // last compressed block
	block := make([]byte, 3+len(blockBody))
	block[0] = byte(hdr)
	block[1] = byte(hdr >> 8)
	block[2] = byte(hdr >> 16)
	copy(block[3:], blockBody)
	return block, true
}

func findMatch(src []byte, i int) (offset, length int) {
	const minMatch = zstdMinMatch
	const maxOffset = 1 << 20
	start := i - maxOffset
	if start < 0 {
		start = 0
	}
	for off := i - 1; off >= start; off-- {
		maxLen := len(src) - i
		if maxLen > 65535+minMatch {
			maxLen = 65535 + minMatch
		}
		l := 0
		for l < maxLen && src[off+l] == src[i+l] {
			l++
		}
		if l >= minMatch && l > length {
			offset = i - off
			length = l
		}
	}
	return offset, length
}

func buildSequences(src []byte) (literals []byte, sequences []seq) {
	const minMatch = zstdMinMatch
	prev := 0
	pos := 0
	recent := recentOffsets
	for pos < len(src) {
		off, ml := findMatch(src, pos)
		if ml < minMatch {
			pos++
			continue
		}
		if pos > prev {
			literals = append(literals, src[prev:pos]...)
		}
		encOff := encodeMatchOffset(uint32(off), uint32(pos-prev), &recent)
		s := seq{
			litLen:   uint32(pos - prev),
			matchLen: uint32(ml - minMatch),
			offset:   encOff,
		}
		s.setCodes()
		sequences = append(sequences, s)
		pos += ml
		prev = pos
	}
	if prev < len(src) {
		literals = append(literals, src[prev:]...)
	}
	return literals, sequences
}
