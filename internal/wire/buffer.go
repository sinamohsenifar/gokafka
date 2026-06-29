package wire

import (
	"encoding/binary"
	"errors"
	"math"
)

var ErrShortBuffer = errors.New("wire: buffer too short")

// Buffer reads and writes Kafka protocol fields (big-endian + compact types).
type Buffer struct {
	B []byte
	I int
}

func NewBuffer(size int) *Buffer {
	return &Buffer{B: make([]byte, 0, size)}
}

func FromBytes(b []byte) *Buffer {
	return &Buffer{B: b}
}

func (b *Buffer) Bytes() []byte { return b.B }
func (b *Buffer) Remaining() []byte {
	if b.I >= len(b.B) {
		return nil
	}
	return b.B[b.I:]
}

func (b *Buffer) ReadInt8() (int8, error) {
	if b.I+1 > len(b.B) {
		return 0, ErrShortBuffer
	}
	v := int8(b.B[b.I])
	b.I++
	return v, nil
}

func (b *Buffer) ReadInt16() (int16, error) {
	if b.I+2 > len(b.B) {
		return 0, ErrShortBuffer
	}
	v := int16(binary.BigEndian.Uint16(b.B[b.I:]))
	b.I += 2
	return v, nil
}

func (b *Buffer) ReadInt32() (int32, error) {
	if b.I+4 > len(b.B) {
		return 0, ErrShortBuffer
	}
	v := int32(binary.BigEndian.Uint32(b.B[b.I:]))
	b.I += 4
	return v, nil
}

func (b *Buffer) ReadInt64() (int64, error) {
	if b.I+8 > len(b.B) {
		return 0, ErrShortBuffer
	}
	v := int64(binary.BigEndian.Uint64(b.B[b.I:]))
	b.I += 8
	return v, nil
}

func (b *Buffer) ReadBool() (bool, error) {
	v, err := b.ReadInt8()
	return v != 0, err
}

func (b *Buffer) ReadString() (string, error) {
	n, err := b.ReadInt16()
	if err != nil {
		return "", err
	}
	if n < 0 {
		return "", nil
	}
	if b.I+int(n) > len(b.B) {
		return "", ErrShortBuffer
	}
	s := string(b.B[b.I : b.I+int(n)])
	b.I += int(n)
	return s, nil
}

func (b *Buffer) ReadBytes() ([]byte, error) {
	n, err := b.ReadInt32()
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, nil
	}
	if b.I+int(n) > len(b.B) {
		return nil, ErrShortBuffer
	}
	out := make([]byte, n)
	copy(out, b.B[b.I:b.I+int(n)])
	b.I += int(n)
	return out, nil
}

func (b *Buffer) ReadCompactString() (string, error) {
	n, err := b.ReadUvarint()
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	size := int(n) - 1
	if b.I+size > len(b.B) {
		return "", ErrShortBuffer
	}
	s := string(b.B[b.I : b.I+size])
	b.I += size
	return s, nil
}

func (b *Buffer) ReadCompactNullableString() (string, error) {
	n, err := b.ReadUvarint()
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", nil
	}
	if n == 1 {
		return "", nil
	}
	size := int(n) - 1
	if b.I+size > len(b.B) {
		return "", ErrShortBuffer
	}
	s := string(b.B[b.I : b.I+size])
	b.I += size
	return s, nil
}

// ReadCompactNullableStringPtr reads a compact nullable string, distinguishing
// null (isNull=true) from an empty string.
func (b *Buffer) ReadCompactNullableStringPtr() (s string, isNull bool, err error) {
	n, err := b.ReadUvarint()
	if err != nil {
		return "", false, err
	}
	if n == 0 {
		return "", true, nil
	}
	size := int(n) - 1
	if size == 0 {
		return "", false, nil
	}
	if b.I+size > len(b.B) {
		return "", false, ErrShortBuffer
	}
	s = string(b.B[b.I : b.I+size])
	b.I += size
	return s, false, nil
}

func (b *Buffer) ReadCompactBytes() ([]byte, error) {
	n, err := b.ReadUvarint()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	size := int(n) - 1
	if b.I+size > len(b.B) {
		return nil, ErrShortBuffer
	}
	out := make([]byte, size)
	copy(out, b.B[b.I:b.I+size])
	b.I += size
	return out, nil
}

func (b *Buffer) ReadUvarint() (uint, error) {
	var x uint
	var s uint
	for i := 0; i < 5; i++ {
		if b.I >= len(b.B) {
			return 0, ErrShortBuffer
		}
		c := b.B[b.I]
		b.I++
		if c < 0x80 {
			if i == 4 && c > 1 {
				return 0, errors.New("wire: uvarint overflow")
			}
			return x | uint(c)<<s, nil
		}
		x |= uint(c&0x7f) << s
		s += 7
	}
	return 0, errors.New("wire: uvarint overflow")
}

func (b *Buffer) ReadVarint() (int, error) {
	u, err := b.ReadUvarint()
	if err != nil {
		return 0, err
	}
	return int(u>>1) ^ -(int(u) & 1), nil
}

func (b *Buffer) SkipTagSection() error {
	n, err := b.ReadUvarint()
	if err != nil || n == 0 {
		return err
	}
	for i := uint(0); i < n; i++ {
		if _, err := b.ReadUvarint(); err != nil { // tag
			return err
		}
		size, err := b.ReadUvarint()
		if err != nil {
			return err
		}
		if b.I+int(size) > len(b.B) {
			return ErrShortBuffer
		}
		b.I += int(size)
	}
	return nil
}

func (b *Buffer) WriteInt8(v int8) {
	b.B = append(b.B, byte(v))
}

func (b *Buffer) WriteInt16(v int16) {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	b.B = append(b.B, tmp[:]...)
}

func (b *Buffer) WriteInt32(v int32) {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	b.B = append(b.B, tmp[:]...)
}

func (b *Buffer) WriteInt64(v int64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
	b.B = append(b.B, tmp[:]...)
}

// ReadFloat64 reads an IEEE-754 big-endian float64 (Kafka float64 type).
func (b *Buffer) ReadFloat64() (float64, error) {
	if b.I+8 > len(b.B) {
		return 0, ErrShortBuffer
	}
	v := math.Float64frombits(binary.BigEndian.Uint64(b.B[b.I:]))
	b.I += 8
	return v, nil
}

// WriteFloat64 writes an IEEE-754 big-endian float64.
func (b *Buffer) WriteFloat64(v float64) {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], math.Float64bits(v))
	b.B = append(b.B, tmp[:]...)
}

func (b *Buffer) WriteBool(v bool) {
	if v {
		b.WriteInt8(1)
	} else {
		b.WriteInt8(0)
	}
}

func (b *Buffer) WriteString(s string) {
	if len(s) > math.MaxInt16 {
		panic("wire: string too long")
	}
	b.WriteInt16(int16(len(s)))
	b.B = append(b.B, s...)
}

func (b *Buffer) WriteNullableString(s *string) {
	if s == nil {
		b.WriteInt16(-1)
		return
	}
	b.WriteString(*s)
}

func (b *Buffer) WriteBytes(v []byte) {
	if v == nil {
		b.WriteInt32(-1)
		return
	}
	b.WriteInt32(int32(len(v)))
	b.B = append(b.B, v...)
}

func (b *Buffer) WriteCompactString(s string) {
	b.WriteUvarint(uint(len(s)) + 1)
	b.B = append(b.B, s...)
}

func (b *Buffer) WriteCompactNullableString(s *string) {
	if s == nil {
		b.WriteUvarint(0)
		return
	}
	b.WriteCompactString(*s)
}

func (b *Buffer) WriteCompactBytes(v []byte) {
	if v == nil {
		b.WriteUvarint(0)
		return
	}
	b.WriteUvarint(uint(len(v)) + 1)
	b.B = append(b.B, v...)
}

func (b *Buffer) WriteCompactArrayLen(n int) {
	b.WriteUvarint(uint(n) + 1)
}

func (b *Buffer) WriteEmptyTagSection() {
	b.WriteUvarint(0)
}

func (b *Buffer) WriteUvarint(x uint) {
	for x >= 0x80 {
		b.B = append(b.B, byte(x)|0x80)
		x >>= 7
	}
	b.B = append(b.B, byte(x))
}

func (b *Buffer) WriteVarint(v int) {
	uv := (uint(v) << 1) ^ uint(v>>63)
	b.WriteUvarint(uv)
}

// PrependLength prefixes a request body with Kafka int32 size field.
func PrependLength(body []byte) []byte {
	out := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(out, uint32(len(body)))
	copy(out[4:], body)
	return out
}
