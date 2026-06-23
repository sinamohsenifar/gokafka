// Portions adapted from github.com/klauspost/compress/zstd (Apache 2.0).

package zstd

var bitMask16 = [32]uint16{
	0, 1, 3, 7, 0xF, 0x1F,
	0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF,
	0xFFF, 0x1FFF, 0x3FFF, 0x7FFF, 0xFFFF, 0xFFFF,
	0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF,
	0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF, 0xFFFF,
}

var bitMask32 = [32]uint32{
	0, 1, 3, 7, 0xF, 0x1F, 0x3F, 0x7F, 0xFF,
	0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF, 0x3FFF, 0x7FFF, 0xFFFF,
	0x1ffff, 0x3ffff, 0x7FFFF, 0xfFFFF, 0x1fFFFF, 0x3fFFFF, 0x7fFFFF, 0xffFFFF,
	0x1ffFFFF, 0x3ffFFFF, 0x7ffFFFF, 0xfffFFFF, 0x1fffFFFF, 0x3fffFFFF, 0x7fffFFFF,
}

type bitWriter struct {
	bitContainer uint64
	nBits        uint8
	out          []byte
}

func (b *bitWriter) reset(out []byte) {
	b.bitContainer = 0
	b.nBits = 0
	b.out = out
}

func (b *bitWriter) addBits16NC(value uint16, bits uint8) {
	b.bitContainer |= uint64(value&bitMask16[bits&31]) << (b.nBits & 63)
	b.nBits += bits
}

func (b *bitWriter) addBits32NC(value uint32, bits uint8) {
	b.bitContainer |= uint64(value&bitMask32[bits&31]) << (b.nBits & 63)
	b.nBits += bits
}

func (b *bitWriter) addBits16Clean(value uint16, bits uint8) {
	b.bitContainer |= uint64(value) << (b.nBits & 63)
	b.nBits += bits
}

func (b *bitWriter) flush32() {
	if b.nBits < 32 {
		return
	}
	b.out = append(b.out,
		byte(b.bitContainer),
		byte(b.bitContainer>>8),
		byte(b.bitContainer>>16),
		byte(b.bitContainer>>24),
	)
	b.nBits -= 32
	b.bitContainer >>= 32
}

func (b *bitWriter) flushAlign() {
	nbBytes := (b.nBits + 7) >> 3
	for i := range nbBytes {
		b.out = append(b.out, byte(b.bitContainer>>(i*8)))
	}
	b.nBits = 0
	b.bitContainer = 0
}

func (b *bitWriter) close() {
	b.addBits16Clean(1, 1)
	b.flushAlign()
}

func (b *bitWriter) bytes() []byte {
	return b.out
}
