package srwire

import (
	"encoding/binary"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// Format identifies Confluent Schema Registry payload framing.
type Format int8

const (
	FormatConfluent Format = 0
)

// Header is the Confluent wire prefix (magic + schema ID).
type Header struct {
	Magic    byte
	SchemaID int32
}

// EncodeConfluent prepends magic byte 0 and big-endian schema ID.
func EncodeConfluent(schemaID int32, payload []byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = byte(FormatConfluent)
	binary.BigEndian.PutUint32(out[1:5], uint32(schemaID))
	copy(out[5:], payload)
	return out
}

// DecodeConfluent strips the Confluent prefix.
func DecodeConfluent(b []byte) (Header, []byte, error) {
	if len(b) < 5 {
		return Header{}, nil, wire.ErrShortBuffer
	}
	if b[0] != byte(FormatConfluent) {
		return Header{}, nil, wire.ErrShortBuffer
	}
	id := int32(binary.BigEndian.Uint32(b[1:5]))
	return Header{Magic: b[0], SchemaID: id}, b[5:], nil
}

// EncodeProtobuf prepends schema ID and Protobuf message indexes per Confluent spec.
// indexes [0] is optimized to a single 0 byte.
func EncodeProtobuf(schemaID int32, indexes []int, payload []byte) []byte {
	idxBytes := encodeMessageIndexes(indexes)
	out := make([]byte, 5+len(idxBytes)+len(payload))
	out[0] = byte(FormatConfluent)
	binary.BigEndian.PutUint32(out[1:5], uint32(schemaID))
	copy(out[5:], idxBytes)
	copy(out[5+len(idxBytes):], payload)
	return out
}

// DecodeProtobuf splits Confluent Protobuf framing.
func DecodeProtobuf(b []byte) (Header, []int, []byte, error) {
	h, rest, err := DecodeConfluent(b)
	if err != nil {
		return Header{}, nil, nil, err
	}
	indexes, n, err := decodeMessageIndexes(rest)
	if err != nil {
		return Header{}, nil, nil, err
	}
	return h, indexes, rest[n:], nil
}

func encodeMessageIndexes(indexes []int) []byte {
	if len(indexes) == 1 && indexes[0] == 0 {
		return []byte{0}
	}
	// Confluent's KafkaProtobufSerializer writes both the count and each index
	// as zigzag varints (ByteUtils.writeVarint); the count is NOT a plain
	// unsigned varint.
	buf := wire.NewBuffer(8)
	buf.WriteVarint(len(indexes))
	for _, ix := range indexes {
		buf.WriteVarint(ix)
	}
	return buf.Bytes()
}

func decodeMessageIndexes(b []byte) ([]int, int, error) {
	buf := wire.FromBytes(b)
	n, err := buf.ReadVarint()
	if err != nil {
		return nil, 0, err
	}
	if n == 0 {
		return []int{0}, buf.I, nil
	}
	out := make([]int, 0, n)
	for i := 0; i < n; i++ {
		v, err := buf.ReadVarint()
		if err != nil {
			return nil, 0, err
		}
		out = append(out, v)
	}
	return out, buf.I, nil
}
