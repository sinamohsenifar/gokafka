//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

// TestIntegrationPartitionReassignments exercises List/Alter partition
// reassignments (KIP-455). On a single-broker cluster there are no ongoing
// reassignments, and reassigning to the current replica (broker 1) is accepted.
func TestIntegrationPartitionReassignments(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cfg, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	cli, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	admin := cli.Admin()

	topic := fmt.Sprintf("gokafka-reassign-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, admin, topic)
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic) })

	// No reassignment in progress for a freshly created topic.
	ongoing, err := admin.ListPartitionReassignments(ctx, map[string][]int32{topic: {0}})
	if err != nil {
		t.Fatalf("list reassignments: %v", err)
	}
	if len(ongoing) != 0 {
		t.Fatalf("expected no ongoing reassignments, got %+v", ongoing)
	}

	// Reassign partition 0 to its current replica. Use a real broker id from the
	// cluster (single-node Kafka is often 1, Redpanda is 0).
	cluster, err := admin.DescribeCluster(ctx)
	if err != nil {
		t.Fatal(err)
	}
	brokerID := cluster.Brokers[0].NodeID
	results, err := admin.AlterPartitionReassignments(ctx, map[string]map[int32][]int32{
		topic: {0: {brokerID}},
	})
	if err != nil {
		t.Fatalf("alter reassignments: %v", err)
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("reassignment %s-%d failed: %v", r.Topic, r.Partition, r.Err)
		}
	}
}
