// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Portions adapted from github.com/klauspost/compress/zstd (Apache 2.0).

package zstd

import "math/bits"

const zstdMinMatch = 3

var (
	llCodeTable = [64]byte{
		0, 1, 2, 3, 4, 5, 6, 7,
		8, 9, 10, 11, 12, 13, 14, 15,
		16, 16, 17, 17, 18, 18, 19, 19,
		20, 20, 20, 20, 21, 21, 21, 21,
		22, 22, 22, 22, 22, 22, 22, 22,
		23, 23, 23, 23, 23, 23, 23, 23,
		24, 24, 24, 24, 24, 24, 24, 24,
		24, 24, 24, 24, 24, 24, 24, 24,
	}
	llBitsTable = [36]byte{
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		1, 1, 1, 1, 2, 2, 3, 3,
		4, 6, 7, 8, 9, 10, 11, 12,
		13, 14, 15, 16,
	}
	mlCodeTable = [128]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
		16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
		32, 32, 33, 33, 34, 34, 35, 35, 36, 36, 36, 36, 37, 37, 37, 37,
		38, 38, 38, 38, 38, 38, 38, 38, 39, 39, 39, 39, 39, 39, 39, 39,
		40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
		41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41, 41,
		42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
		42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
	}
	mlBitsTable = [53]byte{
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0,
		1, 1, 1, 1, 2, 2, 3, 3,
		4, 4, 5, 7, 8, 9, 10, 11,
		12, 13, 14, 15, 16,
	}
)

func highBit32(v uint32) uint32 {
	if v == 0 {
		return 0
	}
	return uint32(bits.Len32(v) - 1)
}

func llCode(litLength uint32) uint8 {
	const llDeltaCode = 19
	if litLength <= 63 {
		return llCodeTable[litLength&63]
	}
	return uint8(highBit32(litLength)) + llDeltaCode
}

func mlCode(mlBase uint32) uint8 {
	const mlDeltaCode = 36
	if mlBase <= 127 {
		return mlCodeTable[mlBase&127]
	}
	return uint8(highBit32(mlBase)) + mlDeltaCode
}

func ofCode(offset uint32) uint8 {
	return uint8(bits.Len32(offset) - 1)
}

type seq struct {
	litLen   uint32
	matchLen uint32
	offset   uint32
	llCode   uint8
	mlCode   uint8
	ofCode   uint8
}

var recentOffsets = [3]uint32{1, 4, 8}

func encodeMatchOffset(offset, lits uint32, recent *[3]uint32) uint32 {
	if lits > 0 {
		switch offset {
		case recent[0]:
			return 1
		case recent[1]:
			recent[1], recent[0] = recent[0], offset
			return 2
		case recent[2]:
			recent[2], recent[1], recent[0] = recent[1], recent[0], offset
			return 3
		default:
			recent[2], recent[1], recent[0] = recent[1], recent[0], offset
			return offset + 3
		}
	}
	switch offset {
	case recent[1]:
		recent[1], recent[0] = recent[0], offset
		return 1
	case recent[2]:
		recent[2], recent[1], recent[0] = recent[1], recent[0], offset
		return 2
	case recent[0] - 1:
		recent[2], recent[1], recent[0] = recent[1], recent[0], offset
		return 3
	default:
		recent[2], recent[1], recent[0] = recent[1], recent[0], offset
		return offset + 3
	}
}

func (s *seq) setCodes() {
	s.llCode = llCode(s.litLen)
	s.mlCode = mlCode(s.matchLen)
	s.ofCode = ofCode(s.offset)
}
