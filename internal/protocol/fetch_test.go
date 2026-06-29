package protocol

import (
	"strings"
	"testing"
)

// A v0/v1 message set (magic 0 or 1) must be rejected, not silently misparsed
// as a v2 RecordBatch.
func TestDecodeOneRecordBatchRejectsLegacyMagic(t *testing.T) {
	batch := make([]byte, 80)
	batch[16] = 1 // magic byte position: baseOffset(8)+batchLength(4)+leaderEpoch(4)
	_, err := decodeOneRecordBatch("t", 0, batch)
	if err == nil || !strings.Contains(err.Error(), "magic") {
		t.Fatalf("expected magic error, got %v", err)
	}
}

// A v2 batch that is too short to contain the fixed header is ignored (nil, nil),
// matching Kafka's partial-trailing-batch semantics.
func TestDecodeOneRecordBatchShortIsIgnored(t *testing.T) {
	recs, err := decodeOneRecordBatch("t", 0, make([]byte, 10))
	if err != nil || recs != nil {
		t.Fatalf("expected (nil,nil) for short batch, got (%v,%v)", recs, err)
	}
}
