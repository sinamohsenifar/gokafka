package gokafka

import (
	"context"
	"testing"

	"github.com/sinamohsenifar/gokafka/kfake"
)

// A produce retry must reuse the partition chosen on the first attempt, not
// re-run the partitioner. RoundRobinPartitioner advances a shared counter per
// call, so re-resolving a keyless record on retry would route it to a DIFFERENT
// partition — reordering it, or (if an earlier attempt committed) duplicating it
// across partitions. Regression test for the partition-freeze fix: with the
// counter seeded to 10 and 3 partitions, the record must land on (10+1)%3 == 2
// even after a retriable produce failure forces one retry (without the freeze it
// would advance to (10+2)%3 == 0).
func TestProduceRetryKeepsFrozenPartition(t *testing.T) {
	broker, err := kfake.NewBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	broker.AddTopic("t", 3)

	rr := &RoundRobinPartitioner{counter: 10}
	cfg, err := NewConfig([]string{broker.Addr()},
		WithProducer(ProducerConfig{Idempotent: true, Acks: AcksAll}),
		WithPartitioner(rr))
	if err != nil {
		t.Fatal(err)
	}
	cli, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	ctx := context.Background()
	prod := cli.Producer()

	// Fault the first Produce (NOT_LEADER_OR_FOLLOWER, retriable) so exactly one
	// retry happens; the batch is not committed on the faulted attempt.
	broker.FailNextProduce(1, 6)

	res, err := prod.ProduceSyncResult(ctx, Record{Topic: "t", Partition: -1, Value: []byte("v")})
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("got %d results, want 1", len(res))
	}
	if res[0].Partition != 2 {
		t.Fatalf("record committed to partition %d, want 2 (partitioner re-ran on retry?)", res[0].Partition)
	}
	// The partitioner must have been consulted exactly once (counter 10 -> 11),
	// not again on the retry.
	if rr.counter != 11 {
		t.Fatalf("round-robin counter = %d, want 11 (partitioner ran %d times, want 1)", rr.counter, rr.counter-10)
	}
}
