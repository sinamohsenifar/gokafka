// Command manualoffsets demonstrates manual consumer offset control in GoKafka:
// where in the log to start reading, how to jump around with Seek, and how to
// commit ONLY the records you actually processed for at-least-once delivery.
//
// The offset is the consumer's bookmark. Two independent mechanisms drive it:
//
//  1. The *reset* policy (WithConsumeFromBeginning / WithConsumeSince) — decides
//     where to start when the group has NO committed offset for a partition
//     (first run, or after retention expired the old commit). It does nothing
//     once a committed offset exists.
//
//  2. Explicit *seeks* (Seek / SeekToBeginning / SeekToEnd / SeekToTime) — move
//     the in-memory read position of an already-assigned partition at any time,
//     overriding both the reset policy and any committed offset until you next
//     commit.
//
// Committing is what makes progress durable across restarts and rebalances.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	log.Printf("gokafka version %s", gokafka.VersionString())

	brokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	topic := env("KAFKA_TOPIC", "gokafka-demo")

	// The reset policy only applies to partitions with NO committed offset yet.
	// WithConsumeFromBeginning => start at earliest; WithConsumeSince(d) =>
	// start at the first record no older than d (KIP-1106), and it takes
	// precedence over WithConsumeFromBeginning. Without either, a fresh group
	// starts at the latest offset (only new records). We show both here; the
	// duration wins.
	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithClientID("gokafka-manualoffsets-example"),
		gokafka.WithConsumerGroup(env("KAFKA_GROUP", "gokafka-manualoffsets-group")),
		gokafka.WithConsumeFromBeginning(true), // read the whole log on first run...
		gokafka.WithConsumeSince(24*time.Hour), // ...but no older than the last 24h.
	)
	if err != nil {
		log.Fatal(err)
	}

	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	consumer := client.Consumer([]string{topic})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// A first Poll triggers the group join and partition assignment, so that the
	// Seek* calls below have partitions to operate on. Seek on an unassigned
	// partition returns an error, so it must come after assignment.
	recs, err := consumer.Poll(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Fatal(err)
	}

	// Inspect what the group assigned us. Each entry carries the current
	// in-memory read offset for that partition.
	assigned := consumer.AssignedPartitions()
	log.Printf("assigned %d partition(s):", len(assigned))
	for _, tp := range assigned {
		log.Printf("  %s-%d @ offset %d", tp.Topic, tp.Partition, tp.Offset)
	}

	// --- Explicit seeks: reposition the read cursor on demand. ---
	//
	// These override the reset policy and any committed offset. The new position
	// takes effect on the NEXT Poll; nothing is committed until you Commit, so a
	// seek is purely a local decision until then.
	if len(assigned) > 0 {
		p := assigned[0].Partition

		// SeekToBeginning: replay this partition from the earliest retained
		// record. Useful for reprocessing / backfills.
		if err := consumer.SeekToBeginning(ctx, topic, p); err != nil {
			log.Fatalf("seek to beginning: %v", err)
		}
		log.Printf("sought %s-%d to beginning (earliest)", topic, p)

		// SeekToTime: resolve, per partition, the first offset whose record
		// timestamp is at or after t, and jump there — "give me everything from
		// one hour ago onward".
		oneHourAgo := time.Now().Add(-time.Hour)
		if err := consumer.SeekToTime(ctx, topic, oneHourAgo, p); err != nil {
			log.Fatalf("seek to time: %v", err)
		}
		log.Printf("sought %s-%d to first record at/after %s", topic, p, oneHourAgo.Format(time.RFC3339))

		// Seek: jump to a specific absolute offset you already know (e.g. one you
		// stored elsewhere). This is a pure in-memory update — no broker call.
		if err := consumer.Seek(topic, p, 0); err != nil {
			log.Fatalf("seek: %v", err)
		}
		log.Printf("sought %s-%d to absolute offset 0", topic, p)

		// SeekToEnd: skip everything already in the log and only read records
		// produced from now on — the classic "tail the topic" position.
		if err := consumer.SeekToEnd(ctx, topic, p); err != nil {
			log.Fatalf("seek to end: %v", err)
		}
		log.Printf("sought %s-%d to end (latest); will read only new records", topic, p)
	}

	// --- Poll / process / commit loop with at-least-once delivery. ---
	//
	// The ordering matters: process a record FULLY, then commit it. If we crash
	// after processing but before committing, the record is redelivered on
	// restart — that is at-least-once (never lost, possibly seen twice, so make
	// processing idempotent). Committing BEFORE processing would instead give
	// at-most-once (lost on crash).
	for {
		if len(recs) == 0 {
			recs, err = consumer.Poll(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Fatal(err)
			}
			if len(recs) == 0 {
				continue
			}
		}

		// Process only the records we can actually handle, and remember exactly
		// which ones succeeded — this is the crux of manual offset control.
		processed := recs[:0:0]
		for _, r := range recs {
			if err := handle(r); err != nil {
				// Stop at the first failure: do NOT commit past a record we
				// could not process, or we would silently skip it. On the next
				// run the group resumes from the last committed offset and
				// redelivers everything from this record onward.
				log.Printf("stopping commit at %s-%d offset %d: %v", r.Topic, r.Partition, r.Offset, err)
				break
			}
			processed = append(processed, r)
		}

		// Commit ONLY the processed records. Passing records explicitly commits
		// each record's offset+1 (the "next to read" position), so it can never
		// commit past a record we didn't finish. Contrast with the no-arg
		// consumer.Commit(ctx): that commits the last offset Poll RETURNED for
		// each partition (the delivered position) and assumes every returned
		// record was processed — convenient, but wrong if you bailed out early.
		if len(processed) > 0 {
			if err := consumer.Commit(ctx, processed...); err != nil {
				log.Fatalf("commit: %v", err)
			}
			last := processed[len(processed)-1]
			log.Printf("committed through %s-%d offset %d (%d record(s))", last.Topic, last.Partition, last.Offset, len(processed))
		}

		recs = nil // force a fresh Poll on the next iteration
	}
}

// handle does the real work for one record. Returning an error leaves the
// record (and everything after it) uncommitted, so it is redelivered later.
func handle(r gokafka.Record) error {
	fmt.Printf("processing partition=%d offset=%d key=%s value=%s\n", r.Partition, r.Offset, r.Key, r.Value)
	return nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
