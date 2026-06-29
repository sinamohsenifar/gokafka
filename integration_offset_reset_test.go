//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

// TestIntegrationConsumeSinceDuration verifies KIP-1106 duration-based offset
// reset: with no committed offset, the consumer starts at the earliest record
// whose timestamp is at or after (now - duration), skipping older records.
func TestIntegrationConsumeSinceDuration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := fmt.Sprintf("gokafka-since-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-since-grp-%d", time.Now().UnixNano())

	pcfg, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	pcli, err := gokafka.NewClient(pcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pcli.Close()
	if err := pcli.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, pcli.Admin(), topic, 1)
	t.Cleanup(func() { _ = pcli.Admin().DeleteTopics(context.Background(), topic) })

	now := time.Now()
	// Two "old" records (2h ago) then two "recent" records (now).
	old := []string{"old-1", "old-2"}
	recent := []string{"recent-1", "recent-2"}
	for _, v := range old {
		if err := pcli.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte(v), Timestamp: now.Add(-2 * time.Hour)}); err != nil {
			t.Fatalf("produce old: %v", err)
		}
	}
	for _, v := range recent {
		if err := pcli.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte(v), Timestamp: now}); err != nil {
			t.Fatalf("produce recent: %v", err)
		}
	}

	// Let the broker's time index catch up so ListOffsets-by-timestamp is exact.
	time.Sleep(2 * time.Second)

	// Reset to the last hour -> should see only the recent records.
	ccfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithConsumerGroup(group),
		gokafka.WithConsumeSince(1*time.Hour),
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
	deadline := time.Now().Add(20 * time.Second)
	for len(seen) < len(recent) && time.Now().Before(deadline) {
		recs, err := cons.Poll(ctx)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		for _, r := range recs {
			seen[string(r.Value)] = true
		}
	}
	for _, v := range recent {
		if !seen[v] {
			t.Fatalf("expected to see recent record %q, seen=%v", v, seen)
		}
	}
	for _, v := range old {
		if seen[v] {
			t.Fatalf("did NOT expect old record %q (before now-1h), seen=%v", v, seen)
		}
	}
}
