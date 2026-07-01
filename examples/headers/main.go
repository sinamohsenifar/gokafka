// Command headers demonstrates GoKafka record headers: small key/value metadata
// carried alongside a record's key and value. Headers are the idiomatic place to
// put out-of-band metadata that a consumer needs BEFORE (or without) touching the
// payload — a W3C trace id for distributed tracing, a "content-type" so the
// consumer knows how to deserialize the value, a schema id / version hint, a
// tenant id for routing, and so on. Unlike the key, headers do not affect
// partitioning; unlike the value, they are conventionally many small typed fields
// rather than one opaque body.
//
// This example produces one record with several headers and then consumes it back
// in the same group, printing each header so you can see the exact bytes survive
// the round trip. Header keys are strings and values are raw []byte — GoKafka does
// not interpret them, so encode/decode is up to you (here we use plain UTF-8).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	log.Printf("gokafka version %s", gokafka.VersionString())

	brokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	topic := env("KAFKA_TOPIC", "gokafka-headers-demo")

	cfg := gokafka.DefaultConfig(brokers)
	cfg.ClientID = "gokafka-headers-example"
	// A group is needed only for the consume-back half of this demo.
	cfg.ConsumerGroup = env("KAFKA_GROUP", "gokafka-headers-group")

	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// --- Produce a record WITH headers -------------------------------------
	//
	// Each header is a gokafka.Header{Key, Value}. The Key is a string; the Value
	// is []byte, so any header carrying a non-string (an int, a UUID, a protobuf
	// tag) must be encoded to bytes by you. Common conventions shown here:
	//   - "content-type"  : how to interpret Value (mirrors HTTP)
	//   - "trace-id"      : a tracing/correlation id to stitch producer→consumer
	//   - "schema-id"     : which schema/version the value was written with
	// Header keys are NOT required to be unique — Kafka preserves duplicates in
	// order, which some ecosystems use to model multi-valued headers.
	rec := gokafka.Record{
		Topic: topic,
		Key:   []byte("order-42"),
		Value: []byte(`{"orderId":42,"total":"19.99"}`),
		Headers: []gokafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "trace-id", Value: []byte("4bf92f3577b34da6a3ce929d0e0e4736")},
			{Key: "schema-id", Value: []byte("orders-v3")},
		},
	}

	results, err := client.Producer().ProduceSyncResult(ctx, rec)
	if err != nil {
		log.Fatal(err)
	}
	if len(results) > 0 {
		log.Printf("produced to %s partition=%d offset=%d with %d header(s)",
			results[0].Topic, results[0].Partition, results[0].Offset, len(rec.Headers))
	}

	// --- Consume it back and read the headers ------------------------------
	//
	// On the way back, GoKafka decodes the wire headers into the same
	// []gokafka.Header slice on each returned Record, preserving key order and the
	// exact value bytes. A consumer typically reads a well-known header first
	// (e.g. dispatch on "content-type") before deciding how to handle Value.
	consumer := client.Consumer([]string{topic})

	// Bound the demo so it exits even if no record is delivered (e.g. the topic
	// was empty or the broker is slow), rather than polling forever.
	pollCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for {
		recs, err := consumer.Poll(pollCtx)
		if err != nil {
			if pollCtx.Err() != nil {
				log.Printf("no record received before timeout; nothing to show")
				return
			}
			log.Fatal(err)
		}
		if len(recs) == 0 {
			continue
		}
		for _, r := range recs {
			fmt.Printf("consumed partition=%d offset=%d key=%s\n", r.Partition, r.Offset, r.Key)
			if len(r.Headers) == 0 {
				fmt.Println("  (no headers)")
			}
			for _, h := range r.Headers {
				// h.Value is []byte; print as text here since our headers are UTF-8.
				fmt.Printf("  header %-13s = %s\n", h.Key, h.Value)
			}
			// Example of using a header to drive processing: pick the deserializer
			// based on content-type instead of guessing from the payload.
			if ct := headerValue(r.Headers, "content-type"); ct != "" {
				fmt.Printf("  -> would deserialize value as %q\n", ct)
			}
		}
		// Commit what we processed so a rerun of this example does not redeliver it.
		if err := consumer.Commit(ctx, recs...); err != nil {
			log.Fatal(err)
		}
		return
	}
}

// headerValue returns the value of the first header with the given key as a
// string, or "" if the record carries no such header. Header lookup is a linear
// scan because a record's header set is small and ordered (there is no map on the
// wire), so callers do their own key matching.
func headerValue(headers []gokafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
