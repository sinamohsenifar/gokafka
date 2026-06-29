package protocol

import (
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// RequestHeader is the standard Kafka request header.
type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
}

func EncodeRequest(h RequestHeader, body []byte) []byte {
	buf := wire.NewBuffer(len(body) + 36)
	buf.B = append(buf.B, 0, 0, 0, 0) // reserved length prefix, back-patched below
	buf.WriteInt16(h.APIKey)
	buf.WriteInt16(h.APIVersion)
	buf.WriteInt32(h.CorrelationID)
	if RequestHeaderVersion(h.APIKey, h.APIVersion) >= 2 {
		// Request header v2 adds tagged fields; ClientId stays a legacy STRING (see RequestHeader.json).
		buf.WriteString(h.ClientID)
		buf.WriteEmptyTagSection()
	} else {
		buf.WriteString(h.ClientID)
	}
	buf.B = append(buf.B, body...)
	out := buf.Bytes()
	wire.PatchLength(out)
	return out
}

// RequestHeaderVersion returns the Kafka request header version for an API call.
func RequestHeaderVersion(apiKey, apiVersion int16) int16 {
	if flexibleRequestHeader(apiKey, apiVersion) {
		return 2
	}
	return 1
}

type ResponseHeader struct {
	CorrelationID int32
}

func DecodeResponseHeader(raw []byte) (ResponseHeader, int, error) {
	if len(raw) < 8 {
		return ResponseHeader{}, 0, wire.ErrShortBuffer
	}
	size := int(int32(raw[0])<<24 | int32(raw[1])<<16 | int32(raw[2])<<8 | int32(raw[3]))
	if len(raw) < 4+size {
		return ResponseHeader{}, 0, wire.ErrShortBuffer
	}
	body := raw[4 : 4+size]
	buf := wire.FromBytes(body)
	id, err := buf.ReadInt32()
	if err != nil {
		return ResponseHeader{}, 0, err
	}
	return ResponseHeader{CorrelationID: id}, 4 + size, nil
}

func ResponseBody(raw []byte) ([]byte, error) {
	return ResponseBodyForAPI(raw, 0, 0)
}

// ResponseBodyForAPI strips the response header and, for flexible APIs, the header tag section.
func ResponseBodyForAPI(raw []byte, apiKey, apiVersion int16) ([]byte, error) {
	if len(raw) < 8 {
		return nil, wire.ErrShortBuffer
	}
	size := int(int32(raw[0])<<24 | int32(raw[1])<<16 | int32(raw[2])<<8 | int32(raw[3]))
	if len(raw) < 4+size {
		return nil, wire.ErrShortBuffer
	}
	body := raw[4 : 4+size]
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // correlation_id
		return nil, err
	}
	if flexibleRequestHeader(apiKey, apiVersion) {
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
	}
	return buf.Remaining(), nil
}
