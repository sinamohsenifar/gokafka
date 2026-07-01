// Command rebalancelistener demonstrates GoKafka's rebalance listener: a hook
// that fires whenever the consumer group reassigns partitions across its members.
//
// WHAT a rebalance listener is:
//
//	When members join or leave a consumer group (or topic metadata changes), the
//	group coordinator redistributes partitions. GoKafka lets you observe that
//	lifecycle by implementing gokafka.RebalanceListener, which has two callbacks:
//	  - OnPartitionsRevoked  fires BEFORE partitions are taken away from you.
//	  - OnPartitionsAssigned fires AFTER new partitions are handed to you.
//	This is the Go equivalent of Java's ConsumerRebalanceListener.
//
// WHY it matters:
//
//	These callbacks are the canonical place to keep per-partition state in sync
//	with ownership. In OnPartitionsRevoked you typically commit offsets and
//	flush/close any per-partition resources (buffers, files, DB batches) BEFORE
//	you lose the partition — otherwise the next owner starts from a stale offset
//	and you risk reprocessing or losing in-flight work. In OnPartitionsAssigned
//	you initialize state for the partitions you just gained (seek to a stored
//	offset, warm a cache, open a writer).
//
// This example uses the COOPERATIVE-STICKY assignor. With cooperative rebalancing
// (incremental cooperative protocol) a rebalance revokes only the partitions that
// actually move to another member instead of the classic "stop-the-world" model
// where everyone drops everything and re-acquires. That means OnPartitionsRevoked
// reports just the partitions you are genuinely losing, so pauses are shorter and
// most partitions keep flowing across a rebalance.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sinamohsenifar/gokafka"
)

// loggingRebalanceListener implements gokafka.RebalanceListener. In a real
// application the callbacks would commit offsets and init/flush per-partition
// state; here we simply log what changed so the rebalance is observable.
type loggingRebalanceListener struct{}

// OnPartitionsRevoked runs just BEFORE these partitions are reassigned away.
// This is where you would commit final offsets and flush per-partition state so
// the next owner resumes cleanly.
func (loggingRebalanceListener) OnPartitionsRevoked(_ context.Context, partitions []gokafka.TopicPartition) {
	if len(partitions) == 0 {
		log.Printf("rebalance: nothing revoked")
		return
	}
	for _, tp := range partitions {
		// tp.Offset carries the consumer's current position for the partition —
		// a natural value to commit before letting the partition go.
		log.Printf("rebalance: REVOKED  %s-%d (position=%d) -> commit/flush here", tp.Topic, tp.Partition, tp.Offset)
	}
}

// OnPartitionsAssigned runs just AFTER these partitions are handed to us. This is
// where you would initialize per-partition state (seek, open writers, warm caches).
func (loggingRebalanceListener) OnPartitionsAssigned(_ context.Context, partitions []gokafka.TopicPartition) {
	if len(partitions) == 0 {
		log.Printf("rebalance: nothing assigned")
		return
	}
	for _, tp := range partitions {
		log.Printf("rebalance: ASSIGNED %s-%d (start=%d) -> init per-partition state here", tp.Topic, tp.Partition, tp.Offset)
	}
}

func main() {
	log.Printf("gokafka version %s", gokafka.VersionString())

	cfg := gokafka.DefaultConfig([]string{env("KAFKA_BROKERS", "localhost:9092")})
	cfg.ClientID = "gokafka-rebalance-listener"
	cfg.ConsumerGroup = env("KAFKA_GROUP", "gokafka-demo-group")

	// Select the cooperative-sticky assignor so rebalances are incremental:
	// only partitions that actually change owner are revoked/assigned. Other
	// choices are AssignorRange, AssignorRoundRobin, and AssignorSticky.
	cfg.Consumer.Assignor = gokafka.AssignorCooperativeSticky

	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topic := env("KAFKA_TOPIC", "gokafka-demo")
	consumer := client.Consumer([]string{topic})

	// Register the listener BEFORE the first poll so the initial assignment (the
	// first join is itself a rebalance) is delivered to OnPartitionsAssigned.
	consumer.SetRebalanceListener(loggingRebalanceListener{})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("joined group %q as cooperative-sticky member; run a second instance to trigger a rebalance", cfg.ConsumerGroup)

	// Drive the consumer. Rebalances happen inside Poll (on join, and whenever
	// group membership changes), which is when the listener callbacks fire.
	for {
		recs, err := consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				// Graceful shutdown: leave the group so the coordinator can
				// rebalance our partitions to the remaining members promptly.
				_ = consumer.Leave(context.Background())
				return
			}
			log.Fatal(err)
		}
		for _, r := range recs {
			log.Printf("consumed %s-%d offset=%d key=%s", r.Topic, r.Partition, r.Offset, r.Key)
		}
		if len(recs) > 0 {
			if err := consumer.Commit(ctx, recs...); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
