package protocol

import (
	"encoding/binary"
	"testing"
)

func TestDecodeOffsetCommitResponseSuccess(t *testing.T) {
	// throttle=0, 1 topic, name "t", 1 partition, part=0, err=0
	body := make([]byte, 0, 32)
	body = appendInt32(body, 0)
	body = appendInt32(body, 1)
	body = appendString(body, "t")
	body = appendInt32(body, 1)
	body = appendInt32(body, 0)
	body = appendInt16(body, 0)

	code, err := DecodeOffsetCommitResponse(7, body)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("code=%d", code)
	}
}

func appendInt32(b []byte, v int32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(v))
	return append(b, buf[:]...)
}

func appendInt16(b []byte, v int16) []byte {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], uint16(v))
	return append(b, buf[:]...)
}

func appendString(b []byte, s string) []byte {
	b = appendInt16(b, int16(len(s)))
	return append(b, s...)
}
