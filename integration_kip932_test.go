//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func TestIntegrationShareConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)
	setup, err := gokafka.NewConfig(brokers)
	if err != nil {
		t.Fatal(err)
	}
	probe, err := gokafka.NewClient(setup)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := probe.NegotiatedAPIVersion(protocol.APIShareGroupHeartbeat); !ok || v == 0 {
		probe.Close()
		t.Skip("broker does not support KIP-932 ShareGroupHeartbeat (Kafka 4.1+ with share.version=1)")
	}
	probe.Close()

	topic := fmt.Sprintf("gokafka-share-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-share-grp-%d", time.Now().UnixNano())

	adminCfg, err := gokafka.NewConfig(brokers)
	if err != nil {
		t.Fatal(err)
	}
	adminClient, err := gokafka.NewClient(adminCfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := adminClient.Admin().CreateTopic(ctx, topic, 2, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, adminClient.Admin(), topic, 2)
	t.Cleanup(func() {
		_ = adminClient.Admin().DeleteTopics(context.Background(), topic)
		adminClient.Close()
	})

	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithShareGroup(group),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	c, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	prod := c.Producer()
	if err := prod.ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte("share-msg")}); err != nil {
		t.Fatal(err)
	}

	share := c.ShareConsumer([]string{topic})
	recs, err := share.Poll(ctx)
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("expected share-acquired records")
	}
	if err := share.Acknowledge(ctx, recs...); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := share.Leave(ctx); err != nil {
		t.Fatalf("leave: %v", err)
	}
}
