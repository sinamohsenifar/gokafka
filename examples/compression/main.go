// Command compression demonstrates producer-side record-batch compression in
// GoKafka. The producer compresses whole record batches (not individual records)
// before sending them to the broker, and consumers transparently decompress on
// read — the codec travels in the batch header, so nothing extra is needed on
// the consume side.
//
// Why compress? Kafka throughput is usually network- and disk-bound, and record
// batches (especially JSON/text) are highly compressible. Trading a little CPU
// for a smaller wire/log footprint typically means higher effective throughput
// and lower storage cost. Compression also improves batching: smaller batches
// fit more records under the broker's message-size limits.
package main

import (
	"context"
	"log"
	"os"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	log.Printf("gokafka version %s", gokafka.VersionString())

	brokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	topic := env("KAFKA_TOPIC", "gokafka-compression-demo")

	// WithProducerCompression selects the codec applied to every produced record
	// batch. GoKafka ships pure-Go encoders for four codecs (plus "none"):
	//
	//   CompressionNone   — no compression; lowest CPU, largest wire size.
	//   CompressionGzip   — best compression RATIO, highest CPU. Honors a level
	//                       (see below). Good for cold/archival topics or when
	//                       network/storage is the bottleneck and CPU is spare.
	//   CompressionSnappy — fast, modest ratio. A classic "safe default".
	//   CompressionLZ4    — very fast, good ratio. Great for high-THROUGHPUT
	//                       pipelines where you want compression to be nearly free.
	//   CompressionZstd   — the modern sweet spot: near-gzip ratios at LZ4-class
	//                       speed (KIP-110). Prefer it for high-throughput topics
	//                       when your whole cluster supports zstd (Kafka 2.1+).
	//
	// Rule of thumb: reach for zstd or lz4 when you care about throughput, and
	// gzip when you care most about the compression ratio (smallest bytes on the
	// wire/disk) and can spend the CPU.
	//
	// WithProducerCompressionLevel is KIP-390: it tunes the codec's effort. It is
	// only honored by gzip here (1 = fastest .. 9 = smallest); the pure-Go
	// snappy/lz4/zstd encoders use a fixed level and ignore it. 0 = codec default.
	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithClientID("gokafka-compression-example"),
		// Zstd is the recommended general-purpose codec: strong ratio, fast.
		gokafka.WithProducerCompression(gokafka.CompressionZstd),
		// Level is a no-op for zstd; shown here to illustrate the knob. Switch the
		// codec above to CompressionGzip to actually see levels take effect (e.g.
		// pass 9 for maximum ratio, 1 for fastest).
		gokafka.WithProducerCompressionLevel(0),
	)
	if err != nil {
		log.Fatal(err)
	}

	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Produce a few sizable, repetitive records. Compression works on the whole
	// batch, so the win grows with batch size and with how compressible the
	// payloads are — repetitive JSON like this compresses extremely well.
	records := make([]gokafka.Record, 0, 5)
	for i := 0; i < 5; i++ {
		records = append(records, gokafka.Record{
			Topic: topic,
			Key:   []byte("compressible"),
			// A ~2 KB highly repetitive payload: the codec collapses the repeated
			// text down to a fraction of its size before it ever hits the wire.
			Value: makeCompressiblePayload(),
		})
	}

	// The codec is applied transparently as the batch is serialized; the call
	// site is identical to an uncompressed produce. ProduceSyncResult blocks
	// until the broker acknowledges and returns the assigned partitions/offsets.
	results, err := client.Producer().ProduceSyncResult(ctx, records...)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range results {
		log.Printf("produced (zstd-compressed) to %s partition=%d offset=%d",
			r.Topic, r.Partition, r.Offset)
	}
	log.Printf("%d records sent as a zstd-compressed batch; consumers decompress transparently", len(results))
}

// makeCompressiblePayload builds a repetitive ~2 KB JSON-ish value. Real-world
// event payloads (shared field names, enums, timestamps) compress similarly well.
func makeCompressiblePayload() []byte {
	const line = `{"event":"page_view","user":"anon","path":"/home","status":"ok"},`
	buf := make([]byte, 0, 2048)
	buf = append(buf, '[')
	for len(buf) < 2000 {
		buf = append(buf, line...)
	}
	buf = append(buf, ']')
	return buf
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
