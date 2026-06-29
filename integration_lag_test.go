//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

// TestIntegrationConsumerGroupLag verifies Admin.ConsumerGroupLag: after
// producing N records and committing at M, the reported lag is N-M.
func TestIntegrationConsumerGroupLag(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := fmt.Sprintf("gokafka-lag-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-lag-grp-%d", time.Now().UnixNano())

	cfg, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	cli, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	if err := cli.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, cli.Admin(), topic)
	t.Cleanup(func() { _ = cli.Admin().DeleteTopics(context.Background(), topic) })

	const total = 10
	recs := make([]gokafka.Record, total)
	for i := range recs {
		recs[i] = gokafka.Record{Topic: topic, Value: []byte(fmt.Sprintf("m-%d", i))}
	}
	if err := cli.Producer().ProduceSync(ctx, recs...); err != nil {
		t.Fatal(err)
	}

	// Consume and commit a prefix of the records.
	ccfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithConsumerGroup(group), gokafka.WithConsumeFromBeginning(true))
	if err != nil {
		t.Fatal(err)
	}
	ccli, err := gokafka.NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ccli.Close()
	cons := ccli.Consumer([]string{topic})

	const commitCount = 4
	var committed []gokafka.Record
	deadline := time.Now().Add(25 * time.Second)
	for len(committed) < commitCount && time.Now().Before(deadline) {
		got, err := cons.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range got {
			if len(committed) < commitCount {
				committed = append(committed, r)
			}
		}
	}
	if len(committed) < commitCount {
		t.Fatalf("consumed only %d/%d records", len(committed), commitCount)
	}
	if err := cons.Commit(ctx, committed...); err != nil {
		t.Fatalf("commit: %v", err)
	}

	lags, err := cli.Admin().ConsumerGroupLag(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	if len(lags) == 0 {
		t.Fatal("no lag entries returned")
	}
	var found bool
	for _, l := range lags {
		if l.Topic != topic {
			continue
		}
		found = true
		if l.LogEndOffset != total {
			t.Errorf("LogEndOffset = %d, want %d", l.LogEndOffset, total)
		}
		if l.Committed != commitCount {
			t.Errorf("Committed = %d, want %d", l.Committed, commitCount)
		}
		if l.Lag != total-commitCount {
			t.Errorf("Lag = %d, want %d", l.Lag, total-commitCount)
		}
	}
	if !found {
		t.Fatalf("topic %s not in lag report: %+v", topic, lags)
	}
}
