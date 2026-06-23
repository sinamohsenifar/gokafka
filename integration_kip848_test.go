//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationConsumerGroup848(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)
	topic := fmt.Sprintf("gokafka-848-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-848-grp-%d", time.Now().UnixNano())

	setup, err := gokafka.NewConfig(brokers)
	if err != nil {
		t.Fatal(err)
	}
	client, err := gokafka.NewClient(setup)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Admin().CreateTopic(ctx, topic, 2, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, client.Admin(), topic, 2)
	t.Cleanup(func() {
		_ = client.Admin().DeleteTopics(context.Background(), topic)
		client.Close()
	})

	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithConsumerGroup(group),
		gokafka.WithGroupProtocol(gokafka.GroupProtocolNextGen),
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

	consumer := c.Consumer([]string{topic})
	if err := consumer.Rebalance(ctx); err != nil {
		t.Skipf("KIP-848 not enabled on broker (set group.coordinator.rebalance.protocol=consumer): %v", err)
	}
	parts := consumer.AssignedPartitions()
	if len(parts) == 0 {
		t.Fatal("expected partition assignment via ConsumerGroupHeartbeat")
	}
	if err := consumer.Leave(ctx); err != nil {
		t.Fatalf("leave: %v", err)
	}
}
