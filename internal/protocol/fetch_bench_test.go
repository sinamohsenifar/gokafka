package protocol

import "testing"

func benchRecordBatch(b *testing.B, n int) []byte {
	b.Helper()
	batch, err := encodeRecordBatch(benchRecords(n), DefaultProduceSettings(), 0)
	if err != nil {
		b.Fatal(err)
	}
	return batch
}

func BenchmarkDecodeRecordBatch1(b *testing.B) {
	batch := benchRecordBatch(b, 1)
	b.SetBytes(int64(len(batch)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decodeRecordBatch("bench-topic", 0, batch, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeRecordBatch1000(b *testing.B) {
	batch := benchRecordBatch(b, 1000)
	b.SetBytes(int64(len(batch)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := decodeRecordBatch("bench-topic", 0, batch, nil); err != nil {
			b.Fatal(err)
		}
	}
}
