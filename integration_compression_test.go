//go:build integration

package gokafka_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func integrationCompressionProduceConsume(t *testing.T, compression gokafka.CompressionCodec) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithProducer(gokafka.ProducerConfig{
			Compression: compression,
			Idempotent:  true,
			Acks:        gokafka.AcksAll,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topic := fmt.Sprintf("gokafka-comp-%d", time.Now().UnixNano())
	if err := client.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, client.Admin(), topic)
	t.Cleanup(func() { _ = client.Admin().DeleteTopics(context.Background(), topic) })

	payload := bytes.Repeat([]byte(fmt.Sprintf("compressed-%v-", compression)), 32)
	if err := client.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: payload}); err != nil {
		t.Fatal(err)
	}

	cfg2, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithConsumerGroup("gokafka-comp-"+time.Now().Format("150405.000")),
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
			if string(r.Value) == string(payload) {
				return
			}
		}
	}
	t.Fatal("compressed record not consumed")
}

func TestIntegrationCompressionGzip(t *testing.T) {
	integrationCompressionProduceConsume(t, gokafka.CompressionGzip)
}

func TestIntegrationCompressionSnappy(t *testing.T) {
	integrationCompressionProduceConsume(t, gokafka.CompressionSnappy)
}

func TestIntegrationCompressionLZ4(t *testing.T) {
	integrationCompressionProduceConsume(t, gokafka.CompressionLZ4)
}

func TestIntegrationCompressionZstd(t *testing.T) {
	integrationCompressionProduceConsume(t, gokafka.CompressionZstd)
}
