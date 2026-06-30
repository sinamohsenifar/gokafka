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

// TestIntegrationBrokerFeatures verifies finalized cluster features are parsed
// from the ApiVersions response. metadata.version is always finalized on a KRaft
// cluster; transaction.version drives KIP-890 TV2 negotiation.
func TestIntegrationBrokerFeatures(t *testing.T) {
	cfg, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	cli, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	mv, ok := cli.BrokerFeature("metadata.version")
	if !ok {
		// Non-Kafka brokers that implement the Kafka protocol (e.g. Redpanda) do
		// not advertise KRaft finalized features. That's fine — BrokerFeature
		// returns not-found and transactions fall back to v1.
		t.Skip("broker does not advertise finalized features (e.g. Redpanda); metadata.version parsing only applies to KRaft Kafka")
	}
	if mv <= 0 {
		t.Fatalf("metadata.version captured but level=%d — feature parsing broken", mv)
	}
	t.Logf("metadata.version finalized level = %d", mv)
	if tv, ok := cli.BrokerFeature("transaction.version"); ok {
		t.Logf("transaction.version finalized level = %d (TV2 available = %v)", tv, tv >= 2)
	} else {
		t.Logf("transaction.version not advertised by this broker")
	}
}
