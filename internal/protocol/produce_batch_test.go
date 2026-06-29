package protocol

import (
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/compress"
)

func TestRecordBatchRoundTrip(t *testing.T) {
	records := []ProduceRecord{
		{Key: []byte("k1"), Value: []byte("v1"), Timestamp: time.UnixMilli(1000)},
		{Key: []byte("k2"), Value: []byte("v2"), Timestamp: time.UnixMilli(2000)},
	}
	settings := DefaultProduceSettings()

	for _, codec := range []int8{compress.CodecNone, compress.CodecGzip, compress.CodecSnappy, compress.CodecLZ4} {
		settings.Compression = codec
		batch, err := encodeRecordBatch(records, settings, 0)
		if err != nil {
			t.Fatalf("codec %d encode: %v", codec, err)
		}
		got, err := decodeRecordBatch("t", 0, batch, nil)
		if err != nil {
			t.Fatalf("codec %d decode: %v", codec, err)
		}
		if len(got) != len(records) {
			t.Fatalf("codec %d: got %d records want %d", codec, len(got), len(records))
		}
		for i, r := range got {
			if string(r.Value) != string(records[i].Value) {
				t.Fatalf("codec %d record %d: %q", codec, i, r.Value)
			}
		}
	}
}
