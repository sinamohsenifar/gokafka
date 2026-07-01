// Command partitioner demonstrates how GoKafka decides WHICH partition a record
// lands on. Partition choice controls two things that matter in production:
//   - ordering: Kafka only orders records WITHIN a partition, so every record
//     that must stay in order (e.g. all events for one user) has to hit the same
//     partition — that is what key-based routing buys you.
//   - interop: in a mixed-client fleet the hash used to map key -> partition must
//     match across producers, or the same key splits across partitions.
//
// It shows the three ways to control routing, in order of precedence:
//  1. Record.Partition >= 0 pins a record to an explicit partition (highest
//     precedence — the partitioner is never consulted).
//  2. A per-client Partitioner (chosen with gokafka.WithPartitioner) routes
//     records whose Partition is left as -1 (auto). Built-ins:
//     - HashPartitioner   (murmur2, Java/Sarama-compatible)  — the DEFAULT
//     - CRC32Partitioner  (librdkafka/kafka-go-compatible)
//     - RoundRobinPartitioner (keyless spreading)
//  3. If no partitioner is set, the default HashPartitioner is used.
//
// IMPORTANT: a plain gokafka.Record{} has Partition == 0, and 0 is a VALID
// explicit partition. So to get automatic key-based routing you must set
// Partition: -1 (any negative value means "auto — let the partitioner decide").
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
	topic := env("KAFKA_TOPIC", "gokafka-partitioner-demo")

	// --- (a) Default routing: HashPartitioner (murmur2) ------------------------
	//
	// With no WithPartitioner option, GoKafka uses HashPartitioner. It hashes the
	// key with Kafka's murmur2 (the exact algorithm the Java DefaultPartitioner
	// and librdkafka's consistent partitioner use), so a given key always maps to
	// the same partition — and to the SAME partition other Java/Sarama producers
	// would choose. That consistency is what makes keyed ordering and log-compacted
	// topics work across a mixed-client fleet.
	//
	// Note: records with an empty/nil key all go to partition 0 under a hash
	// partitioner; use RoundRobinPartitioner (below) to spread keyless records.
	log.Printf("--- (a) default HashPartitioner (murmur2, Java-compatible) ---")
	hashCfg, err := gokafka.NewConfig(brokers, gokafka.WithClientID("gokafka-part-hash"))
	if err != nil {
		log.Fatal(err)
	}
	hashClient, err := gokafka.NewClient(hashCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer hashClient.Close()

	// Set Partition: -1 so the partitioner actually runs. The same key ("user-42")
	// twice must resolve to the same partition — that is the whole point.
	produce(hashClient, "key-routed (hash)", []gokafka.Record{
		{Topic: topic, Partition: -1, Key: []byte("user-42"), Value: []byte(`{"n":1}`)},
		{Topic: topic, Partition: -1, Key: []byte("user-42"), Value: []byte(`{"n":2}`)}, // same key -> same partition
		{Topic: topic, Partition: -1, Key: []byte("user-7"), Value: []byte(`{"n":3}`)},
		{Topic: topic, Partition: -1, Key: []byte("order-99"), Value: []byte(`{"n":4}`)},
	})

	// --- (b) Choosing a different partitioner ---------------------------------
	//
	// gokafka.WithPartitioner swaps the routing strategy for the whole client.
	//
	// RoundRobinPartitioner ignores the key and cycles through partitions, evenly
	// spreading load. Use it when you have no key (or don't need ordering) and just
	// want balanced partitions. It is stateful but safe for concurrent producers.
	log.Printf("--- (b1) RoundRobinPartitioner (ignores key, spreads evenly) ---")
	rrCfg, err := gokafka.NewConfig(brokers,
		gokafka.WithClientID("gokafka-part-rr"),
		gokafka.WithPartitioner(&gokafka.RoundRobinPartitioner{}),
	)
	if err != nil {
		log.Fatal(err)
	}
	rrClient, err := gokafka.NewClient(rrCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer rrClient.Close()

	produce(rrClient, "round-robin", []gokafka.Record{
		{Topic: topic, Partition: -1, Value: []byte(`{"rr":1}`)},
		{Topic: topic, Partition: -1, Value: []byte(`{"rr":2}`)},
		{Topic: topic, Partition: -1, Value: []byte(`{"rr":3}`)},
		{Topic: topic, Partition: -1, Value: []byte(`{"rr":4}`)},
	})

	// CRC32Partitioner routes by CRC32(key), matching librdkafka's consistent
	// partitioner and kafka-go's CRC32Balancer. Reach for it when you need the same
	// key to land where a C/C++/Python/.NET/Go(librdkafka) producer would put it.
	// (For Java/Sarama interop, stick with the default HashPartitioner instead —
	// murmur2 and CRC32 generally pick DIFFERENT partitions for the same key.)
	log.Printf("--- (b2) CRC32Partitioner (librdkafka-compatible) ---")
	crcCfg, err := gokafka.NewConfig(brokers,
		gokafka.WithClientID("gokafka-part-crc"),
		gokafka.WithPartitioner(gokafka.CRC32Partitioner{}),
	)
	if err != nil {
		log.Fatal(err)
	}
	crcClient, err := gokafka.NewClient(crcCfg)
	if err != nil {
		log.Fatal(err)
	}
	defer crcClient.Close()

	produce(crcClient, "key-routed (crc32)", []gokafka.Record{
		{Topic: topic, Partition: -1, Key: []byte("user-42"), Value: []byte(`{"n":1}`)},
		{Topic: topic, Partition: -1, Key: []byte("user-7"), Value: []byte(`{"n":2}`)},
	})

	// --- (c) Pinning a record to an explicit partition ------------------------
	//
	// Setting Record.Partition to a concrete value (>= 0) bypasses the partitioner
	// entirely — GoKafka sends the record straight to that partition. Use this when
	// YOU own the partitioning scheme (e.g. sharding by a computed hash, or routing
	// a control message to a known partition). Here we reuse the hash client, but
	// because Partition is pinned the partitioner is ignored regardless of the key.
	log.Printf("--- (c) explicit Record.Partition (partitioner bypassed) ---")
	produce(hashClient, "explicit-pin", []gokafka.Record{
		{Topic: topic, Partition: 0, Key: []byte("user-42"), Value: []byte(`{"pinned":0}`)},
		{Topic: topic, Partition: 1, Key: []byte("user-42"), Value: []byte(`{"pinned":1}`)},
	})
}

// produce sends the records and prints the partition the broker acknowledged for
// each. ProduceSyncResult returns one ProduceRecordResult per record, whose
// Partition field is the partition GoKafka actually routed to — that is what lets
// you observe the partitioner's decision.
func produce(client *gokafka.Client, label string, records []gokafka.Record) {
	results, err := client.Producer().ProduceSyncResult(context.Background(), records...)
	if err != nil {
		log.Fatalf("%s: produce failed: %v", label, err)
	}
	for i, r := range results {
		log.Printf("[%s] key=%q -> partition=%d offset=%d", label, records[i].Key, r.Partition, r.Offset)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
