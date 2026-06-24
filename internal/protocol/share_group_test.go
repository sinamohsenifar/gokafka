package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestEncodeShareGroupHeartbeatRequest(t *testing.T) {
	body := protocol.EncodeShareGroupHeartbeatRequest(protocol.ShareGroupHeartbeatRequest{
		GroupID:              "share-grp",
		MemberID:             "550e8400-e29b-41d4-a716-446655440000",
		MemberEpoch:          0,
		SubscribedTopicNames: []string{"events"},
	})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}

func TestEncodeShareFetchRequest(t *testing.T) {
	body := protocol.EncodeShareFetchRequest(protocol.VerShareFetch, protocol.ShareFetchRequest{
		GroupID: "g", MemberID: "m", ShareSessionEpoch: 0,
		MaxWaitMs: 500, MinBytes: 1, MaxBytes: 1 << 20, MaxRecords: 100, BatchSize: 1,
	})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}
