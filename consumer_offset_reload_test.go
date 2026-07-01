package gokafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/kfake"
)

// A transient OffsetFetch error (or omission) for an assigned partition must NOT
// leave that partition at the applyAssignment default of offset 0 — which would
// silently re-read the whole partition from the log start (mass duplication).
// The consumer must retry the OffsetFetch until the real committed offset is
// resolved. Regression for the rebalance/offset-fetch data-duplication finding.
func TestConsumerRetriesTransientOffsetFetch(t *testing.T) {
	b, err := kfake.NewBroker()
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.AddTopic("t", 1)

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	// Produce 6 records (offsets 0..5).
	pcfg, _ := gokafka.NewConfig([]string{b.Addr()})
	pcli, _ := gokafka.NewClient(pcfg)
	defer pcli.Close()
	for i := 0; i < 6; i++ {
		if err := pcli.Producer().ProduceSync(ctx, gokafka.Record{Topic: "t", Value: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}

	// Consumer A: consume all 6 and commit — committed offset advances to 6.
	acfg, _ := gokafka.NewConfig([]string{b.Addr()},
		gokafka.WithConsumerGroup("g"), gokafka.WithConsumeFromBeginning(true))
	acli, _ := gokafka.NewClient(acfg)
	consA := acli.Consumer([]string{"t"})
	var all []gokafka.Record
	deadline := time.Now().Add(10 * time.Second)
	for len(all) < 6 && time.Now().Before(deadline) {
		recs, err := consA.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, recs...)
	}
	if len(all) < 6 {
		t.Fatalf("consumer A consumed %d, want 6", len(all))
	}
	if err := consA.Commit(ctx, all...); err != nil {
		t.Fatal(err)
	}
	acli.Close()

	// Inject one transient UNSTABLE_OFFSET_COMMIT (88) into the next OffsetFetch,
	// exactly the window a fresh consumer hits while joining.
	b.FailNextOffsetFetch(1, 88)

	// Consumer B (same group) must resume at the committed offset 6 — returning
	// zero records — not re-read from offset 0.
	bcfg, _ := gokafka.NewConfig([]string{b.Addr()},
		gokafka.WithConsumerGroup("g"), gokafka.WithConsumeFromBeginning(true))
	bcli, _ := gokafka.NewClient(bcfg)
	defer bcli.Close()
	consB := bcli.Consumer([]string{"t"})

	var got int
	deadline = time.Now().Add(6 * time.Second)
	for i := 0; i < 3 && time.Now().Before(deadline); i++ {
		recs, err := consB.Poll(ctx)
		if err != nil {
			t.Fatalf("consumer B poll: %v", err)
		}
		got += len(recs)
	}
	if got != 0 {
		t.Fatalf("consumer B re-read %d records after a transient OffsetFetch error; want 0 (must resume at the committed offset, not offset 0)", got)
	}
}
