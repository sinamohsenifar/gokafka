// Command asyncproducer demonstrates GoKafka's AsyncProducer: a high-throughput,
// pipelined way to publish many records without blocking on each broker
// round-trip. Instead of calling ProduceSync once per record and waiting for the
// ack, you feed records into an input channel; a pool of worker goroutines
// accumulates them into batches (honouring the configured BatchSize/Linger) and
// produces them concurrently, while delivery reports stream back on a separate
// results channel. This decouples the rate at which your application generates
// records from the latency of the broker, which is what unlocks throughput.
package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	log.Printf("gokafka version %s", gokafka.VersionString())

	cfg := gokafka.DefaultConfig([]string{env("KAFKA_BROKERS", "localhost:9092")})
	cfg.ClientID = "gokafka-asyncproducer"

	// Concurrency tuning for the async pipeline. ProducerWorkers is the number of
	// goroutines that drain the input channel and produce in parallel — more
	// workers means more in-flight batches to different partitions/brokers.
	// ChannelBuffer sizes both the input and results channels, so a burst of
	// Send() calls doesn't block the caller while workers catch up.
	cfg.Concurrency.ProducerWorkers = 4
	cfg.Concurrency.ChannelBuffer = 1024

	// BatchSize / Linger control how workers coalesce records: a worker flushes a
	// batch once it holds BatchSize records OR once Linger elapses since the first
	// record in the batch, whichever comes first. Bigger batches amortize the
	// per-request overhead (better throughput) at the cost of a little latency.
	cfg.Producer.BatchSize = 200

	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topic := env("KAFKA_TOPIC", "gokafka-demo")

	// Create the async producer. It shares the client's single idempotent
	// producer state, so records still get exactly-once-per-partition sequencing
	// if idempotence is enabled on the config.
	async := client.NewAsyncProducer()

	// Run starts the worker pool and blocks until ctx is cancelled or Close() is
	// called; it closes the results channel when the last worker exits. Run it in
	// its own goroutine so main can keep feeding records and reading results.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go async.Run(ctx)

	const total = 1000

	// Drain the results channel in a separate goroutine. Every produced record
	// yields exactly one ProduceResult carrying the original Record, the broker
	// acknowledgement (topic/partition/offset), and a delivery error if any. We
	// MUST consume these — the workers can block on a full results channel — and
	// they let us observe delivery outcomes (e.g. count successes vs failures).
	done := make(chan struct{})
	go func() {
		defer close(done)
		var delivered, failed int
		// Ranging over Results() ends when Run() closes the channel after Close().
		for res := range async.Results() {
			if res.Err != nil {
				failed++
				log.Printf("delivery failed for key=%s: %v", res.Record.Key, res.Err)
				continue
			}
			delivered++
			// res.Result holds the broker-assigned partition and offset.
			if delivered <= 3 || delivered == total {
				log.Printf("delivered key=%s -> partition=%d offset=%d",
					res.Record.Key, res.Result.Partition, res.Result.Offset)
			}
		}
		log.Printf("done: %d delivered, %d failed", delivered, failed)
	}()

	// Send many records at high throughput. Send() enqueues onto the input
	// channel and returns as soon as the record is buffered — it does NOT wait for
	// the broker ack (that arrives later on Results()). This non-blocking handoff
	// is what lets one caller keep the whole worker pool busy. You could equally
	// push straight onto async.Input(), a chan<- Record, if you don't need Send's
	// context-cancellation handling.
	for i := 0; i < total; i++ {
		rec := gokafka.Record{
			Topic: topic,
			Key:   []byte("key-" + strconv.Itoa(i)),
			Value: []byte(`{"n":` + strconv.Itoa(i) + `}`),
		}
		if err := async.Send(ctx, rec); err != nil {
			log.Fatalf("send: %v", err)
		}
	}

	// Close stops accepting new records and closes the input channel. Workers
	// flush whatever they still hold, emit the final delivery reports, then exit —
	// which makes Run() close the results channel and our drain goroutine finish.
	async.Close()

	// Wait for all delivery reports to be processed before exiting.
	<-done

	// ---------------------------------------------------------------------------
	// Alternative: client.NewBatchProducer() offers the same batching benefit with
	// a simpler, synchronous API when you don't need a full pipeline. You call
	// Send(ctx, record) to accumulate records (it auto-flushes at BatchSize or
	// after Linger), Flush(ctx) to force-send what's pending, and Close(ctx) to
	// flush the remainder before shutdown. Send/Flush return the produce error
	// directly instead of reporting it on a channel, so there's no Results() to
	// drain — trade the async throughput for straight-line control flow.
	// ---------------------------------------------------------------------------
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
