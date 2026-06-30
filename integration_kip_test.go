//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationStaticMembership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)
	topic := fmt.Sprintf("gokafka-static-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-static-grp-%d", time.Now().UnixNano())
	instanceID := fmt.Sprintf("instance-%d", time.Now().UnixNano())

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
		gokafka.WithGroupInstanceID(instanceID),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	c1, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()

	consumer := c1.Consumer([]string{topic})
	if err := consumer.Rebalance(ctx); err != nil {
		t.Fatal(err)
	}
	parts := consumer.AssignedPartitions()
	if len(parts) == 0 {
		t.Fatal("expected partition assignment")
	}

	// Reconnect with same group.instance.id — should reclaim assignment without full rebalance storm.
	c1.Close()
	time.Sleep(500 * time.Millisecond)

	c2, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	consumer2 := c2.Consumer([]string{topic})
	if err := consumer2.Rebalance(ctx); err != nil {
		t.Fatal(err)
	}
	parts2 := consumer2.AssignedPartitions()
	if len(parts2) == 0 {
		t.Fatal("expected reassigned partitions")
	}
}

func TestIntegrationCooperativeStickyRebalance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)
	topic := fmt.Sprintf("gokafka-coop-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-coop-grp-%d", time.Now().UnixNano())

	setup, err := gokafka.NewConfig(brokers)
	if err != nil {
		t.Fatal(err)
	}
	adminClient, err := gokafka.NewClient(setup)
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
		gokafka.WithClientID("coop-1"),
		gokafka.WithConsumerGroup(group),
		gokafka.WithConsumer(gokafka.ConsumerConfig{Assignor: gokafka.AssignorCooperativeSticky}),
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

	cons := c.Consumer([]string{topic})
	if err := cons.Rebalance(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := cons.Poll(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationAlterTopicConfigs(t *testing.T) {
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

	topic := fmt.Sprintf("gokafka-alter-%d", time.Now().UnixNano())
	if err := client.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, client.Admin(), topic)
	t.Cleanup(func() { _ = client.Admin().DeleteTopics(context.Background(), topic) })

	// AlterConfigs v1 legacy wire.
	retention := "86400000"
	if err := client.Admin().AlterTopicConfigs(ctx, map[string][]gokafka.TopicConfigAlteration{
		topic: {{Name: "retention.ms", Value: &retention}},
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	cfgs, err := client.Admin().DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range cfgs[topic] {
		if e.Name == "retention.ms" && e.Value == retention {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("retention.ms not updated: %+v", cfgs[topic])
	}
}

func TestIntegrationIncrementalAlterTopicConfigs(t *testing.T) {
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

	topic := fmt.Sprintf("gokafka-incr-%d", time.Now().UnixNano())
	if err := client.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, client.Admin(), topic)
	t.Cleanup(func() { _ = client.Admin().DeleteTopics(context.Background(), topic) })

	retention := "43200000"
	if err := client.Admin().IncrementalAlterTopicConfigs(ctx, map[string][]gokafka.TopicConfigAlteration{
		topic: {{Name: "retention.ms", Value: &retention}},
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	cfgs, err := client.Admin().DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range cfgs[topic] {
		if e.Name == "retention.ms" && e.Value == retention {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("retention.ms not updated: %+v", cfgs[topic])
	}
}

func TestIntegrationTopicRetentionConfig(t *testing.T) {
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

	topic := fmt.Sprintf("gokafka-incr-%d", time.Now().UnixNano())
	if err := client.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, client.Admin(), topic)
	t.Cleanup(func() { _ = client.Admin().DeleteTopics(context.Background(), topic) })

	retention := "43200000"
	if err := client.Admin().AlterTopicConfigs(ctx, map[string][]gokafka.TopicConfigAlteration{
		topic: {{Name: "retention.ms", Value: &retention}},
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	cfgs, err := client.Admin().DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range cfgs[topic] {
		if e.Name == "retention.ms" && e.Value == retention {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("retention.ms not updated: %+v", cfgs[topic])
	}
}

func TestIntegrationDescribeBrokerConfigs(t *testing.T) {
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

	// Use a real broker id from the cluster (single-node Kafka is often 1,
	// Redpanda is 0) rather than hardcoding one.
	cluster, err := client.Admin().DescribeCluster(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(cluster.Brokers) == 0 {
		t.Fatal("no brokers")
	}
	brokerID := cluster.Brokers[0].NodeID
	cfgs, err := client.Admin().DescribeBrokerConfigs(ctx, brokerID)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfgs[brokerID]) == 0 {
		t.Fatal("expected broker config entries")
	}
}
