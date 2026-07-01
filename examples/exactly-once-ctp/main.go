// Command exactly-once-ctp demonstrates an exactly-once consume-transform-produce
// (CTP) pipeline with GoKafka.
//
// CTP is the canonical exactly-once-semantics (EOS) use case: a stream processor
// reads from an input topic, transforms each record, and writes the result to an
// output topic. The hard part is the boundary between "I produced the output" and
// "I committed the input offset". If those two steps are not atomic you get either
// duplicates (offset committed after a crash that lost the output) or data loss
// (offset committed before the output was durable). Kafka solves this by folding
// BOTH the output records AND the consumed input offsets into a single transaction:
// either everything commits together, or nothing does.
//
// This example is stronger than the plain `transactions` example, which only
// produces inside a transaction. Here we additionally call SendOffsetsToTxn so the
// consumed input offsets are committed as part of the same transaction — the piece
// that actually makes the pipeline exactly-once.
//
// The flow per batch is:
//
//	BeginTransaction                     -> start a txn (init/fence the producer id)
//	ProduceWithinTxn(transformed...)     -> stage the transformed output records
//	SendOffsetsToTxn(group, offsets, md) -> stage the consumed input offsets,
//	                                        stamped with the consumer's group
//	                                        generation + member id so a fenced
//	                                        (zombie) member cannot commit
//	Commit                               -> atomically publish output + offsets
//	                                        (or Abort to roll back both)
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	log.Printf("gokafka version %s", gokafka.VersionString())

	brokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	inTopic := env("KAFKA_TOPIC", "gokafka-ctp-in")
	outTopic := env("KAFKA_OUTPUT_TOPIC", "gokafka-ctp-out")
	group := env("KAFKA_GROUP", "gokafka-ctp-group")

	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithClientID("gokafka-ctp-example"),
		// The consumer group whose offsets we will commit transactionally.
		gokafka.WithConsumerGroup(group),
		// Read only committed data. In a CTP pipeline the input topic is itself the
		// output of some upstream transactional producer, so we must skip aborted
		// records to avoid transforming data that was rolled back.
		gokafka.WithConsumer(gokafka.ConsumerConfig{
			IsolationLevel: gokafka.IsolationReadCommitted,
		}),
		// A transactional id is required for EOS; it fences zombie producers so a
		// stalled-then-resumed instance cannot commit stale output/offsets. In a
		// multi-instance deployment this id must be STABLE per input partition set
		// so recovery fences the previous incarnation.
		gokafka.WithTransaction(gokafka.TransactionConfig{
			Enabled:         true,
			TransactionalID: "gokafka-ctp-1",
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	consumer := client.Consumer([]string{inTopic})
	producer := client.Producer()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	for {
		// 1. Read a batch from the input topic.
		recs, err := consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Fatal(err)
		}
		if len(recs) == 0 {
			continue
		}

		// Process the batch inside one transaction. On ANY error we abort and let
		// the loop re-poll: because the offsets were never committed, the same
		// input is re-delivered and re-processed — but no partial output ever
		// became visible to read_committed consumers, so there is no duplication.
		if err := processBatch(ctx, producer, consumer, group, outTopic, recs); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("batch failed, will retry: %v", err)
			continue
		}
		log.Printf("committed %d transformed records + input offsets atomically", len(recs))
	}
}

// processBatch runs one consume-transform-produce transaction: transform the input
// records, produce the results, commit the consumed offsets, and commit the txn.
func processBatch(
	ctx context.Context,
	producer *gokafka.Producer,
	consumer *gokafka.Consumer,
	group, outTopic string,
	recs []gokafka.Record,
) error {
	// Begin the transaction. This initializes (and fences) the producer id.
	txn, err := producer.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// 2. Transform each input record into an output record. Here the "transform"
	//    is trivial (uppercase the value and re-key by topic), but this is where a
	//    real pipeline would enrich, filter, or reshape the payload.
	out := make([]gokafka.Record, 0, len(recs))
	for _, r := range recs {
		out = append(out, gokafka.Record{
			Topic: outTopic,
			Key:   r.Key,
			Value: []byte(fmt.Sprintf("transformed:%s", r.Value)),
		})
	}

	// 3. Produce the transformed records INTO the transaction. They are staged,
	//    not yet visible to read_committed consumers until Commit.
	if err := txn.ProduceWithinTxn(ctx, out...); err != nil {
		// Roll back: nothing produced so far becomes visible.
		_ = txn.Abort(ctx)
		return fmt.Errorf("produce within txn: %w", err)
	}

	// 4. Build the input offsets to commit: for each (topic, partition) we commit
	//    the offset of the NEXT record to read, i.e. lastConsumed+1. Committing +1
	//    is what tells the group "everything up to and including this record is
	//    done"; committing the record's own offset would re-deliver it on restart.
	offsets := nextOffsets(recs)

	// The generation + member id fence the offset commit: if this consumer was
	// kicked out of the group and a new member took over its partitions, the
	// coordinator rejects this stale commit (FENCED). This is the anti-zombie
	// guarantee that keeps CTP exactly-once across rebalances.
	generation, memberID, groupInstanceID := consumer.GroupMetadata()

	// 5. Commit the consumed input offsets INSIDE the same transaction. This is the
	//    step that plain "transactional produce" lacks — it binds the output and
	//    the input progress into one atomic unit.
	if err := txn.SendOffsetsToTxn(ctx, group, offsets, gokafka.TxnOffsetCommitOptions{
		Generation:      generation,
		MemberID:        memberID,
		GroupInstanceID: groupInstanceID,
	}); err != nil {
		_ = txn.Abort(ctx)
		return fmt.Errorf("send offsets to txn: %w", err)
	}

	// 6. Commit atomically. On success, the transformed output AND the advanced
	//    input offsets become durable together. If the process crashes before this
	//    returns, the transaction is aborted broker-side and the whole batch is
	//    reprocessed from the last committed offset — exactly-once end to end.
	if err := txn.Commit(ctx); err != nil {
		// A commit error leaves the outcome uncertain; try to abort, then report.
		// (Abort returns ErrTransactionAborted on the happy path, which we ignore.)
		if aerr := txn.Abort(ctx); aerr != nil && !errors.Is(aerr, gokafka.ErrTransactionAborted) {
			log.Printf("abort after failed commit: %v", aerr)
		}
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// nextOffsets groups the consumed records by topic and partition and returns the
// offset to commit for each — the highest consumed offset + 1 (the next position
// to read). The shape (map[topic]map[partition]offset) matches SendOffsetsToTxn.
func nextOffsets(recs []gokafka.Record) map[string]map[int32]int64 {
	offsets := make(map[string]map[int32]int64)
	for _, r := range recs {
		parts := offsets[r.Topic]
		if parts == nil {
			parts = make(map[int32]int64)
			offsets[r.Topic] = parts
		}
		if next := r.Offset + 1; next > parts[r.Partition] {
			parts[r.Partition] = next
		}
	}
	return offsets
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
