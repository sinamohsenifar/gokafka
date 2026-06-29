package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// TestEncodeOffsetFetchRequireStableV7 verifies the v7 flexible request carries
// require_stable after the topics array (KIP-447), and that v6 does not.
func TestEncodeOffsetFetchRequireStableV7(t *testing.T) {
	parts := []protocol.OffsetFetchPartition{{Topic: "t", Partition: 0}}

	// v7 with require_stable=true.
	body := protocol.EncodeOffsetFetchRequest(7, "g", "", parts, true)
	buf := wire.FromBytes(body)
	if _, err := buf.ReadCompactString(); err != nil { // group_id
		t.Fatal(err)
	}
	nTopics, err := buf.ReadUvarint() // topics
	if err != nil || nTopics != 2 {
		t.Fatalf("topics len uvarint = %d err=%v", nTopics, err)
	}
	if _, err := buf.ReadCompactString(); err != nil { // topic name
		t.Fatal(err)
	}
	nParts, _ := buf.ReadUvarint()
	for i := 1; i < int(nParts); i++ {
		_, _ = buf.ReadInt32()
	}
	if err := buf.SkipTagSection(); err != nil { // topic tag
		t.Fatal(err)
	}
	rs, err := buf.ReadInt8() // require_stable
	if err != nil {
		t.Fatal(err)
	}
	if rs != 1 {
		t.Fatalf("v7 require_stable = %d, want 1", rs)
	}

	// v6 must NOT write require_stable: the byte after the topic tag is the
	// request tag section (uvarint 0), not a bool.
	body6 := protocol.EncodeOffsetFetchRequest(6, "g", "", parts, true)
	if len(body6) >= len(body) {
		t.Fatalf("v6 body (%d) should be shorter than v7 (%d) — no require_stable byte", len(body6), len(body))
	}
}
