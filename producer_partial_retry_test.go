package gokafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/kfake"
)

// A produce that partially fails must re-send ONLY the failed partition on
// retry, never a partition the broker already committed. With idempotence off
// (no broker sequence dedup) the old code re-sent the whole batch on any
// per-partition failure, duplicating the committed partition's records. Here
// partition 1 is faulted once while partition 0 commits; after recovery
// partition 0 must hold exactly its original records, not double.
func TestProducePartialFailureNoDuplication(t *testing.T) {
	broker, err := kfake.NewBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	broker.AddTopic("t", 2)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Idempotence OFF: no broker-side sequence dedup, so any re-send of an
	// already-committed partition is visible as duplicate records.
	cfg, err := gokafka.NewConfig([]string{broker.Addr()},
		gokafka.WithProducer(gokafka.ProducerConfig{Idempotent: false, Acks: gokafka.AcksAll}))
	if err != nil {
		t.Fatal(err)
	}
	cli, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// Fault partition 1 on the next produce; partition 0 commits normally.
	broker.FailNextProducePartition(1, 6, "t", 1) // 6 = NOT_LEADER_OR_FOLLOWER (retriable)

	recs := []gokafka.Record{
		{Topic: "t", Partition: 0, Value: []byte("a0")},
		{Topic: "t", Partition: 0, Value: []byte("a1")},
		{Topic: "t", Partition: 0, Value: []byte("a2")},
		{Topic: "t", Partition: 1, Value: []byte("b0")},
		{Topic: "t", Partition: 1, Value: []byte("b1")},
	}
	if err := cli.Producer().ProduceSync(ctx, recs...); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// Consume everything and count per partition.
	ccfg, err := gokafka.NewConfig([]string{broker.Addr()},
		gokafka.WithConsumerGroup("g"), gokafka.WithConsumeFromBeginning(true))
	if err != nil {
		t.Fatal(err)
	}
	ccli, err := gokafka.NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ccli.Close()
	cons := ccli.Consumer([]string{"t"})

	perPart := map[int32]int{}
	deadline := time.Now().Add(10 * time.Second)
	total := 0
	for total < 5 && time.Now().Before(deadline) {
		got, err := cons.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range got {
			perPart[r.Partition]++
			total++
		}
	}

	if perPart[0] != 3 {
		t.Fatalf("partition 0 has %d records, want 3 (the committed partition was re-sent on retry — duplication)", perPart[0])
	}
	if perPart[1] != 2 {
		t.Fatalf("partition 1 has %d records, want 2", perPart[1])
	}
}
