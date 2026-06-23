package wire

import "encoding/binary"

// UUID is a 16-byte Kafka topic/member identifier.
type UUID [16]byte

func (u UUID) IsZero() bool {
	return u == UUID{}
}

// ReadUUID reads a big-endian UUID (two int64).
func (b *Buffer) ReadUUID() (UUID, error) {
	var u UUID
	if b.I+16 > len(b.B) {
		return u, ErrShortBuffer
	}
	copy(u[:], b.B[b.I:b.I+16])
	b.I += 16
	return u, nil
}

// WriteUUID writes a UUID as two big-endian int64 values.
func (b *Buffer) WriteUUID(u UUID) {
	var tmp [16]byte
	copy(tmp[:], u[:])
	b.WriteInt64(int64(binary.BigEndian.Uint64(tmp[0:8])))
	b.WriteInt64(int64(binary.BigEndian.Uint64(tmp[8:16])))
}
