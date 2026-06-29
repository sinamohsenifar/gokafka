//go:build multibroker

// Multi-broker and leader-failover tests against the 3-broker KRaft cluster in
// docker-compose.multibroker.yml. Run with:
//
//	docker compose -f docker-compose.multibroker.yml up -d
//	KAFKA_MULTI_BROKERS=127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094 \
//	  go test -tags=multibroker -run TestMultiBroker -timeout 5m -v .
package gokafka_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func multiBrokers(t *testing.T) []string {
	v := os.Getenv("KAFKA_MULTI_BROKERS")
	if v == "" {
		t.Skip("KAFKA_MULTI_BROKERS not set; start docker-compose.multibroker.yml")
	}
	return strings.Split(v, ",")
}

// mbWaitReady waits until the topic reports the expected partition count with
// elected leaders.
func mbWaitReady(t *testing.T, admin *gokafka.Admin, topic string, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < 60; i++ {
		desc, err := admin.DescribeTopic(ctx, topic)
		if err == nil && len(desc.Partitions) == n {
			ready := true
			for _, p := range desc.Partitions {
				if p.Leader < 0 {
					ready = false
				}
			}
			if ready {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("topic %s not ready with %d partitions", topic, n)
}

// TestMultiBrokerProduceConsume verifies produce/consume across a 3-broker
// cluster with a replicated, multi-partition topic (partitions are led by
// different brokers, exercising per-leader request routing).
func TestMultiBrokerProduceConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	brokers := multiBrokers(t)

	cfg, err := gokafka.NewConfig(brokers)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	topic := fmt.Sprintf("gokafka-mb-%d", time.Now().UnixNano())
	if err := cli.Admin().CreateTopics(ctx, gokafka.TopicSpec{Name: topic, Partitions: 6, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	mbWaitReady(t, cli.Admin(), topic, 6)
	t.Cleanup(func() { _ = cli.Admin().DeleteTopics(context.Background(), topic) })

	const n = 60
	for i := 0; i < n; i++ {
		if err := cli.Producer().ProduceSync(ctx, gokafka.Record{
			Topic: topic, Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte(fmt.Sprintf("v%d", i)),
		}); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	ccfg, err := gokafka.NewConfig(brokers,
		gokafka.WithConsumerGroup(fmt.Sprintf("mb-grp-%d", time.Now().UnixNano())),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	ccli, err := gokafka.NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ccli.Close()
	cons := ccli.Consumer([]string{topic})

	seen := map[string]bool{}
	deadline := time.Now().Add(60 * time.Second)
	for len(seen) < n && time.Now().Before(deadline) {
		recs, err := cons.Poll(ctx)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		for _, r := range recs {
			seen[string(r.Value)] = true
		}
	}
	if len(seen) != n {
		t.Fatalf("expected %d records across the cluster, got %d", n, len(seen))
	}
}

// TestMultiBrokerLeaderFailover produces, kills the partition leader, and
// verifies the producer recovers (metadata refresh + retry to the new leader)
// and the consumer still reads every record.
func TestMultiBrokerLeaderFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	brokers := multiBrokers(t)

	cfg, err := gokafka.NewConfig(brokers)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	topic := fmt.Sprintf("gokafka-failover-%d", time.Now().UnixNano())
	if err := cli.Admin().CreateTopics(ctx, gokafka.TopicSpec{Name: topic, Partitions: 1, ReplicationFactor: 3}); err != nil {
		t.Fatal(err)
	}
	mbWaitReady(t, cli.Admin(), topic, 1)
	t.Cleanup(func() { _ = cli.Admin().DeleteTopics(context.Background(), topic) })

	for i := 0; i < 10; i++ {
		if err := cli.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte(fmt.Sprintf("pre-%d", i))}); err != nil {
			t.Fatalf("pre-produce %d: %v", i, err)
		}
	}

	desc, err := cli.Admin().DescribeTopic(ctx, topic)
	if err != nil || len(desc.Partitions) == 0 {
		t.Fatalf("describe topic: %v", err)
	}
	leader := desc.Partitions[0].Leader
	container := fmt.Sprintf("gokafka-mb-%d", leader)
	t.Logf("stopping partition leader broker %d (%s)", leader, container)
	if out, err := exec.Command("docker", "stop", container).CombinedOutput(); err != nil {
		t.Skipf("cannot stop broker container (docker unavailable?): %v: %s", err, out)
	}
	t.Cleanup(func() { _, _ = exec.Command("docker", "start", container).CombinedOutput() })

	// After the leader dies, produce must recover to the newly elected leader.
	recovered := 0
	for i := 0; i < 20; i++ {
		if err := cli.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte(fmt.Sprintf("post-%d", i))}); err != nil {
			t.Fatalf("post-failover produce %d failed (no recovery): %v", i, err)
		}
		recovered++
	}
	t.Logf("produced %d records after leader failover", recovered)

	ccfg, err := gokafka.NewConfig(brokers,
		gokafka.WithConsumerGroup(fmt.Sprintf("failover-grp-%d", time.Now().UnixNano())),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	ccli, err := gokafka.NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ccli.Close()
	cons := ccli.Consumer([]string{topic})

	seen := map[string]bool{}
	deadline := time.Now().Add(60 * time.Second)
	for len(seen) < 30 && time.Now().Before(deadline) {
		recs, err := cons.Poll(ctx)
		if err != nil {
			continue // tolerate transient errors during failover
		}
		for _, r := range recs {
			seen[string(r.Value)] = true
		}
	}
	if len(seen) != 30 {
		t.Fatalf("expected all 30 records (10 pre + 20 post failover), got %d", len(seen))
	}
}
