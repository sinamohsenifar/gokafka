package protocol_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestEncodeMetadataRequestAllTopics(t *testing.T) {
	body := protocol.EncodeMetadataRequest(8, nil)
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}

func TestEncodeMetadataRequestNamedTopics(t *testing.T) {
	body := protocol.EncodeMetadataRequest(8, []string{"events"})
	if len(body) < 10 {
		t.Fatalf("body too short: %d", len(body))
	}
}

func TestEncodeMetadataRequestV12AllTopicsHex(t *testing.T) {
	body := protocol.EncodeMetadataRequest(12, nil)
	t.Logf("v12 all-topics metadata req len=%d hex=%x", len(body), body)
	if len(body) < 4 {
		t.Fatalf("too short: %d", len(body))
	}
}
