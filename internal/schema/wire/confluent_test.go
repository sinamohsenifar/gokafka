package srwire

import (
	"bytes"
	"testing"
)

// Golden bytes from Confluent's KafkaProtobufSerializer: the index count and
// each index are zigzag varints. [0] collapses to a single 0x00 byte.
func TestProtobufIndexGoldenBytes(t *testing.T) {
	cases := []struct {
		indexes []int
		want    []byte
	}{
		{[]int{0}, []byte{0x00}},                // optimized single first-message
		{[]int{5}, []byte{0x02, 0x0a}},          // count=1 (zz=2), index=5 (zz=10)
		{[]int{1, 0}, []byte{0x04, 0x02, 0x00}}, // count=2 (zz=4), 1(zz=2), 0(zz=0)
	}
	for _, c := range cases {
		got := encodeMessageIndexes(c.indexes)
		if !bytes.Equal(got, c.want) {
			t.Errorf("encodeMessageIndexes(%v) = % x, want % x", c.indexes, got, c.want)
		}
		round, _, err := decodeMessageIndexes(got)
		if err != nil {
			t.Fatalf("decode %v: %v", c.indexes, err)
		}
		want := c.indexes
		if len(round) != len(want) {
			t.Fatalf("round %v != %v", round, want)
		}
		for i := range want {
			if round[i] != want[i] {
				t.Fatalf("round %v != %v", round, want)
			}
		}
	}
}

func TestProtobufIndexOptimization(t *testing.T) {
	idx := encodeMessageIndexes([]int{0})
	if len(idx) != 1 || idx[0] != 0 {
		t.Fatalf("got %v", idx)
	}
	round, n, err := decodeMessageIndexes(append(idx, 1, 2, 3))
	if err != nil || n != 1 || len(round) != 1 || round[0] != 0 {
		t.Fatalf("round=%v n=%d err=%v", round, n, err)
	}
}

func TestConfluentRoundTrip(t *testing.T) {
	payload := []byte("data")
	w := EncodeConfluent(99, payload)
	h, raw, err := DecodeConfluent(w)
	if err != nil || h.SchemaID != 99 || string(raw) != "data" {
		t.Fatalf("h=%+v raw=%q err=%v", h, raw, err)
	}
}
