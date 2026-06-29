package protocol

import (
	"encoding/binary"
	"strings"
	"testing"
)

// v2RecordBatchHeader writes the fixed 61-byte RecordBatch header into batch.
func v2RecordBatchHeader(batch []byte, baseOffset int64, attributes int16, lastOffsetDelta int32) {
	binary.BigEndian.PutUint64(batch[0:8], uint64(baseOffset))
	binary.BigEndian.PutUint32(batch[8:12], uint32(len(batch)-12)) // batchLength (bytes after this field)
	batch[16] = 2                                                  // magic
	binary.BigEndian.PutUint16(batch[21:23], uint16(attributes))
	binary.BigEndian.PutUint32(batch[23:27], uint32(lastOffsetDelta))
}

// A control batch (isControl bit 0x20) is reported as a control batch carrying
// the absolute last offset and no records, so the consumer can advance past it.
func TestDecodeOneRecordBatchControlMarker(t *testing.T) {
	batch := make([]byte, 80)
	v2RecordBatchHeader(batch, 41, 0x20, 0)
	info, err := decodeOneRecordBatch("t", 3, batch)
	if err != nil {
		t.Fatal(err)
	}
	if info == nil || !info.isControl || info.lastOffset != 41 || info.records != nil {
		t.Fatalf("expected control batch info at offset 41, got %+v", info)
	}
}

// decodeRecordBatch drops aborted-transaction records (matching producer id and
// offset range) while still advancing past them via a control marker.
func TestDecodeRecordBatchFiltersAborted(t *testing.T) {
	// One transactional data batch at base offset 0 from producer 7.
	batch := make([]byte, 80)
	v2RecordBatchHeader(batch, 0, 0x10, 0)                     // isTransactional
	binary.BigEndian.PutUint64(batch[43:51], uint64(int64(7))) // producerId field
	// Not aborted -> delivered as data (0 records here, but not a control marker).
	recs, err := decodeRecordBatch("t", 0, append([]byte(nil), batch...), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if r.Control {
			t.Fatalf("non-aborted transactional batch should not be a control marker")
		}
	}
	// Same batch, but producer 7 is in the aborted list from offset 0.
	recs, err = decodeRecordBatch("t", 0, append([]byte(nil), batch...), []abortedTxn{{producerID: 7, firstOffset: 0}})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 || !recs[0].Control {
		t.Fatalf("aborted batch should yield only an advance marker, got %+v", recs)
	}
}

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
