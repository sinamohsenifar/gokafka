package kfake_test

import (
	"context"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/kfake"
)

func newClient(t *testing.T, b *kfake.Broker) *gokafka.Client {
	t.Helper()
	cfg, err := gokafka.NewConfig([]string{b.Addr()})
	if err != nil {
		t.Fatal(err)
	}
	cli, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatalf("connect to mock broker: %v", err)
	}
	return cli
}

func TestConnectAndMetadata(t *testing.T) {
	b, err := kfake.NewBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.AddTopic("events", 2)

	cli := newClient(t, b)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topics, err := cli.Admin().ListTopics(ctx)
	if err != nil {
		t.Fatalf("list topics: %v", err)
	}
	if !contains(topics, "events") {
		t.Fatalf("expected topic 'events' in %v", topics)
	}
}

func TestAdminCreateDelete(t *testing.T) {
	b, _ := kfake.NewBroker()
	defer b.Close()
	cli := newClient(t, b)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cli.Admin().CreateTopic(ctx, "orders", 3, 1); err != nil {
		t.Fatalf("create: %v", err)
	}
	topics, err := cli.Admin().ListTopics(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(topics, "orders") {
		t.Fatalf("orders not listed: %v", topics)
	}
	if err := cli.Admin().DeleteTopics(ctx, "orders"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	topics, _ = cli.Admin().ListTopics(ctx)
	if contains(topics, "orders") {
		t.Fatal("orders still present after delete")
	}
}

func TestProduceAssignsOffsets(t *testing.T) {
	b, _ := kfake.NewBroker()
	defer b.Close()
	b.AddTopic("t", 1)
	cli := newClient(t, b)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := cli.Producer().ProduceSyncResult(ctx,
		gokafka.Record{Topic: "t", Value: []byte("a")},
		gokafka.Record{Topic: "t", Value: []byte("b")},
		gokafka.Record{Topic: "t", Value: []byte("c")},
	)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(res) != 3 || res[0].Offset != 0 || res[2].Offset != 2 {
		t.Fatalf("offsets: %+v, want 0..2", res)
	}
}

func TestProduceConsumeRoundTrip(t *testing.T) {
	b, _ := kfake.NewBroker()
	defer b.Close()
	b.AddTopic("events", 1)

	pcli := newClient(t, b)
	defer pcli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 5; i++ {
		if err := pcli.Producer().ProduceSync(ctx, gokafka.Record{Topic: "events", Value: []byte{byte('0' + i)}}); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	ccfg, _ := gokafka.NewConfig([]string{b.Addr()},
		gokafka.WithConsumerGroup("g1"), gokafka.WithConsumeFromBeginning(true))
	ccli, err := gokafka.NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ccli.Close()

	cons := ccli.Consumer([]string{"events"})
	var got []string
	deadline := time.Now().Add(8 * time.Second)
	for len(got) < 5 && time.Now().Before(deadline) {
		recs, err := cons.Poll(ctx)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		for _, r := range recs {
			got = append(got, string(r.Value))
		}
	}
	if len(got) != 5 {
		t.Fatalf("consumed %d records, want 5: %v", len(got), got)
	}
}

// TestCommitAndLag drives the offset-commit + admin-lag path entirely against
// the mock: produce 4, consume+commit, then ConsumerGroupLag reports lag 0.
func TestCommitAndLag(t *testing.T) {
	b, _ := kfake.NewBroker()
	defer b.Close()
	b.AddTopic("q", 1)

	cli := newClient(t, b)
	defer cli.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 0; i < 4; i++ {
		if err := cli.Producer().ProduceSync(ctx, gokafka.Record{Topic: "q", Value: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}

	ccfg, _ := gokafka.NewConfig([]string{b.Addr()},
		gokafka.WithConsumerGroup("lag-grp"), gokafka.WithConsumeFromBeginning(true))
	ccli, _ := gokafka.NewClient(ccfg)
	defer ccli.Close()
	cons := ccli.Consumer([]string{"q"})

	var all []gokafka.Record
	deadline := time.Now().Add(8 * time.Second)
	for len(all) < 4 && time.Now().Before(deadline) {
		recs, err := cons.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, recs...)
	}
	if len(all) < 4 {
		t.Fatalf("consumed %d, want 4", len(all))
	}
	if err := cons.Commit(ctx, all...); err != nil {
		t.Fatalf("commit: %v", err)
	}

	lags, err := cli.Admin().ConsumerGroupLag(ctx, "lag-grp")
	if err != nil {
		t.Fatal(err)
	}
	var total int64 = -1
	for _, l := range lags {
		if l.Topic == "q" {
			total = l.Lag
			if l.LogEndOffset != 4 || l.Committed != 4 {
				t.Fatalf("lag entry %+v, want LEO=4 committed=4", l)
			}
		}
	}
	if total != 0 {
		t.Fatalf("lag = %d, want 0", total)
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}
