//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationHeadersRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := gokafka.NewConfig(integrationBrokers(t), gokafka.WithClientID("gokafka-headers"))
	if err != nil {
		t.Fatal(err)
	}
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topic := fmt.Sprintf("gokafka-hdr-%d", time.Now().UnixNano())
	admin := client.Admin()
	if err := admin.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, admin, topic)
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic) })

	ts := time.Now().UTC().Truncate(time.Millisecond)
	rec := gokafka.HeaderRecord(topic, []byte("payload"),
		gokafka.Header{Key: "trace-id", Value: []byte("abc-123")},
		gokafka.Header{Key: "content-type", Value: []byte("application/json")},
	)
	rec.Timestamp = ts

	if err := client.Producer().ProduceSync(ctx, rec); err != nil {
		t.Fatal(err)
	}

	cfg2, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithConsumerGroup("gokafka-hdr-"+time.Now().Format("150405.000")),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	cclient, err := gokafka.NewClient(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer cclient.Close()

	consumer := cclient.Consumer([]string{topic})
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		recs, err := consumer.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if string(r.Value) != "payload" {
				continue
			}
			v, ok := r.GetHeader("trace-id")
			if !ok || string(v) != "abc-123" {
				t.Fatalf("trace-id=%q ok=%v", v, ok)
			}
			if r.Timestamp.IsZero() {
				t.Fatal("expected timestamp")
			}
			return
		}
	}
	t.Fatal("did not consume header record")
}

func TestIntegrationBatchProduce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topic := fmt.Sprintf("gokafka-batch-%d", time.Now().UnixNano())
	if err := client.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, client.Admin(), topic)
	t.Cleanup(func() { _ = client.Admin().DeleteTopics(context.Background(), topic) })

	recs := make([]gokafka.Record, 10)
	for i := range recs {
		recs[i] = gokafka.Record{Topic: topic, Value: []byte(fmt.Sprintf("msg-%d", i))}
	}
	results, err := client.Producer().ProduceSyncResult(ctx, recs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 10 {
		t.Fatalf("results=%d", len(results))
	}
}
