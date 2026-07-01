// Command idempotent demonstrates GoKafka's idempotent producer: the broker
// deduplicates and reorders-protects records on retry, so a produce that is
// transparently retried after a network hiccup lands exactly once per partition
// and in the original order — no duplicates, no reordering.
//
// How it works: when Idempotent is enabled the producer obtains a producer id
// (PID) from the broker and tags every record with (PID, epoch, sequence). The
// broker tracks the last sequence it accepted per (PID, partition). A retried
// batch carries the SAME sequence numbers, so the broker recognizes it as a
// duplicate and acknowledges it without re-appending — giving you exactly-once
// delivery semantics *per partition* for free.
//
// Two requirements to keep in mind:
//   - Acks must be AcksAll (-1). Idempotence relies on the leader having durably
//     replicated the batch before acking; weaker acks can't guarantee the
//     dedup state survives a leader failover.
//   - GoKafka reserves ONE sequence number per record. Sequences are assigned in
//     produce order and must be contiguous per partition, which is exactly what
//     lets the broker detect gaps (data loss) and duplicates (retries).
//
// Idempotence is nearly free: it adds no extra round-trips on the hot path — just
// the PID fields on each batch — so there is almost never a reason to leave it off.
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
	topic := env("KAFKA_TOPIC", "gokafka-idempotent-demo")

	// Enable the idempotent producer. Idempotent:true turns on PID-based
	// dedup/ordering, and AcksAll is the required acknowledgement level for it.
	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithClientID("gokafka-idempotent-example"),
		gokafka.WithProducer(gokafka.ProducerConfig{
			Idempotent: true,
			Acks:       gokafka.AcksAll, // -1: leader waits for all in-sync replicas
		}),
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

	// A batch of records. Because the producer is idempotent, each record is
	// assigned its own sequence number under the shared PID. If the underlying
	// send is retried, the broker matches these sequences and appends each record
	// exactly once — even across the retry.
	batch := []gokafka.Record{
		{Topic: topic, Key: []byte("order-1"), Value: []byte(`{"order":1,"item":"widget"}`)},
		{Topic: topic, Key: []byte("order-2"), Value: []byte(`{"order":2,"item":"gadget"}`)},
		{Topic: topic, Key: []byte("order-3"), Value: []byte(`{"order":3,"item":"gizmo"}`)},
		{Topic: topic, Key: []byte("order-4"), Value: []byte(`{"order":4,"item":"sprocket"}`)},
	}

	// ProduceSyncResult blocks until the broker acks and returns one result per
	// record, in the same order as the input batch. Each result carries the final
	// partition and offset the broker assigned — the offset advances by one per
	// record within a partition, reflecting the one-sequence-per-record contract.
	results, err := client.Producer().ProduceSyncResult(ctx, batch...)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("idempotently produced %d records (exactly once per partition):", len(results))
	for _, r := range results {
		log.Printf("  topic=%s partition=%d offset=%d key=%s", r.Topic, r.Partition, r.Offset, r.Record.Key)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
