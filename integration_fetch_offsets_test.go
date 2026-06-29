//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

// TestIntegrationFetchOffsetsMultiGroup verifies the batched OffsetFetch API
// (KIP-709): two consumer groups each commit an offset, and a single
// Admin.FetchOffsets call returns the committed offsets for both groups.
func TestIntegrationFetchOffsetsMultiGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := fmt.Sprintf("gokafka-fo-%d", time.Now().UnixNano())
	g1 := fmt.Sprintf("gokafka-fo-g1-%d", time.Now().UnixNano())
	g2 := fmt.Sprintf("gokafka-fo-g2-%d", time.Now().UnixNano())

	setup, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	sclient, err := gokafka.NewClient(setup)
	if err != nil {
		t.Fatal(err)
	}
	defer sclient.Close()
	if err := sclient.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, sclient.Admin(), topic)
	t.Cleanup(func() { _ = sclient.Admin().DeleteTopics(context.Background(), topic) })

	// Produce a few records.
	recs := make([]gokafka.Record, 5)
	for i := range recs {
		recs[i] = gokafka.Record{Topic: topic, Value: []byte(fmt.Sprintf("m-%d", i))}
	}
	if err := sclient.Producer().ProduceSync(ctx, recs...); err != nil {
		t.Fatal(err)
	}

	// Each group consumes and commits.
	consumeAndCommit := func(group string) {
		cfg, err := gokafka.NewConfig(integrationBrokers(t),
			gokafka.WithConsumerGroup(group), gokafka.WithConsumeFromBeginning(true))
		if err != nil {
			t.Fatal(err)
		}
		cl, err := gokafka.NewClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		cons := cl.Consumer([]string{topic})
		deadline := time.Now().Add(20 * time.Second)
		var last []gokafka.Record
		for time.Now().Before(deadline) {
			got, err := cons.Poll(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) > 0 {
				last = got
				break
			}
		}
		if len(last) == 0 {
			t.Fatalf("group %s consumed nothing", group)
		}
		if err := cons.Commit(ctx, last...); err != nil {
			t.Fatalf("group %s commit: %v", group, err)
		}
	}
	consumeAndCommit(g1)
	consumeAndCommit(g2)

	// Batched fetch for both groups.
	res, err := sclient.Admin().FetchOffsets(ctx, g1, g2)
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range []string{g1, g2} {
		offs, ok := res[g]
		if !ok || len(offs) == 0 {
			t.Fatalf("group %s missing from batched FetchOffsets result: %v", g, res)
		}
		var committed bool
		for _, o := range offs {
			if o.Topic == topic && o.ErrorCode == 0 && o.Offset > 0 {
				committed = true
			}
		}
		if !committed {
			t.Fatalf("group %s has no committed offset for %s: %+v", g, topic, offs)
		}
	}
}
