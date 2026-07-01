// Command observability demonstrates GoKafka's built-in observability: metrics
// and structured logging.
//
// GoKafka ships a native in-process metrics Collector plus pluggable hooks and a
// log/slog bridge, so you can wire the client into your existing telemetry stack
// (Prometheus, OpenTelemetry, ELK) without any third-party dependency in the
// client itself. This example shows three complementary pieces:
//
//  1. WithMetrics(true, "gokafka") — turns on the built-in Collector. Every
//     produce/consume/broker request updates atomic counters you can snapshot at
//     any time via Client.Metrics().
//  2. WithMetricsHook(recorder) — fans every metric out to your OWN
//     observe.MetricsRecorder (IncCounter/RecordHistogram/SetGauge). This is how
//     you bridge into Prometheus/OTel: implement the three methods and forward.
//  3. WithSlogHandler(handler) — routes all of GoKafka's internal structured logs
//     (connect, metadata refresh, errors, ...) through a caller-supplied
//     slog.Handler, so client logs land in the same pipeline as your app logs.
//
// After producing and consuming a couple of records, it prints the metrics
// snapshot so you can see the counters that were recorded.
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/observe"
)

// countingRecorder is a minimal custom metrics sink. It implements
// observe.MetricsRecorder (the same interface a Prometheus or OpenTelemetry
// bridge would implement) by simply counting how many of each metric kind it
// saw. A real implementation would translate (name, value, labels) into the
// corresponding Prometheus/OTel instrument. GoKafka calls these methods for you
// whenever the built-in Collector emits — you never call them yourself.
type countingRecorder struct {
	counters   atomic.Int64
	histograms atomic.Int64
	gauges     atomic.Int64
}

func (r *countingRecorder) IncCounter(name string, value int64, labels map[string]string) {
	r.counters.Add(1)
}

func (r *countingRecorder) RecordHistogram(name string, value float64, labels map[string]string) {
	r.histograms.Add(1)
}

func (r *countingRecorder) SetGauge(name string, value float64, labels map[string]string) {
	r.gauges.Add(1)
}

// Compile-time proof the type satisfies the interface GoKafka expects.
var _ observe.MetricsRecorder = (*countingRecorder)(nil)

func main() {
	log.Printf("gokafka version %s", gokafka.VersionString())

	brokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	topic := env("KAFKA_TOPIC", "gokafka-observability-demo")

	// A caller-provided slog.Handler. GoKafka's internal logs will be encoded by
	// THIS handler, so they share your app's format and destination. Here we use
	// a JSON handler at Info level writing to stderr; swap in your own to route
	// GoKafka logs into your logging pipeline.
	logHandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	// Our custom metrics sink. It receives a copy of every metric the built-in
	// Collector emits, in addition to the Collector keeping its own snapshot.
	recorder := &countingRecorder{}

	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithClientID("gokafka-observability-example"),
		// (1) Enable the built-in metrics Collector under the "gokafka" namespace.
		// The namespace prefixes exported metric names (e.g. gokafka_produce_records_total).
		gokafka.WithMetrics(true, "gokafka"),
		// (2) Fan metrics out to our own recorder — this is the bridge point for
		// Prometheus/OTel. You can register more than one hook.
		gokafka.WithMetricsHook(recorder),
		// (3) Route GoKafka's structured logs through our slog.Handler.
		gokafka.WithSlogHandler(logHandler),
	)
	if err != nil {
		log.Fatal(err)
	}

	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// You can also emit your own structured logs through the same logger that
	// GoKafka uses internally, keeping application and client logs consistent.
	client.Log(context.Background(), gokafka.LogLevelInfo,
		"starting observability demo",
		gokafka.StringAttr("topic", topic),
	)

	ctx := context.Background()

	// Produce a couple of records. Each successful produce increments the
	// built-in produce_records_total / produce_bytes_total counters, and those
	// increments are also forwarded to our countingRecorder.
	for i := 0; i < 3; i++ {
		results, err := client.Producer().ProduceSyncResult(ctx, gokafka.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf(`{"n":%d}`, i)),
		})
		if err != nil {
			log.Fatalf("produce: %v", err)
		}
		if len(results) > 0 {
			log.Printf("produced partition=%d offset=%d", results[0].Partition, results[0].Offset)
		}
	}

	// Consume a little so the consume_* counters get exercised too. We bound the
	// poll with a short timeout so the example terminates even if there is
	// nothing new to read.
	consumer := client.Consumer([]string{topic})
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	recs, err := consumer.Poll(pollCtx)
	if err != nil && pollCtx.Err() == nil {
		log.Fatalf("poll: %v", err)
	}
	log.Printf("consumed %d record(s)", len(recs))

	// Snapshot the built-in Collector. Client.Metrics() returns a point-in-time
	// copy of the counters — cheap and safe to call at any time (e.g. from a
	// /metrics HTTP handler or a periodic reporter).
	m := client.Metrics()
	log.Printf("=== metrics snapshot ===")
	log.Printf("produced records : %d", m.Produced)
	log.Printf("produced bytes   : %d", m.BytesProduced)
	log.Printf("produce errors   : %d", m.ProduceErrors)
	log.Printf("consumed records : %d", m.Consumed)
	log.Printf("consumed bytes   : %d", m.BytesConsumed)
	log.Printf("consume errors   : %d", m.ConsumeErrors)
	log.Printf("broker requests  : %d (errors=%d)", m.BrokerRequests, m.BrokerReqErrors)
	log.Printf("avg request      : %.3f ms", m.AvgRequestMillis)

	// The custom hook saw the same metric emissions the Collector recorded.
	log.Printf("=== custom hook fan-out ===")
	log.Printf("hook counters observed   : %d", recorder.counters.Load())
	log.Printf("hook histograms observed : %d", recorder.histograms.Load())
	log.Printf("hook gauges observed     : %d", recorder.gauges.Load())
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
