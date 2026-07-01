// Command sharegroupadmin demonstrates the KIP-932 share-group *admin* surface:
// per-partition start offsets (SPSO), share-group lag, resetting offsets, group
// configs, and the delivery-count / dead-letter pattern. Requires a Kafka 4.1+
// broker with share.version enabled.
//
//	go run ./examples/sharegroupadmin
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	brokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	topic := env("KAFKA_TOPIC", "gokafka-shareadmin-demo")
	group := env("KAFKA_SHARE_GROUP", "gokafka-shareadmin-1")

	// Implicit ack mode auto-accepts each Poll's batch on the next Poll/Leave, and
	// WithShareAutoOffsetReset controls where a fresh group starts.
	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithShareGroup(group),
		gokafka.WithShareAutoOffsetReset("earliest"),
		gokafka.WithShareAcknowledgementMode(gokafka.ShareAckImplicit),
	)
	if err != nil {
		log.Fatal(err)
	}
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	admin := client.Admin()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if err := admin.CreateTopic(ctx, topic, 1, 1); err != nil {
		log.Printf("create topic (may already exist): %v", err)
	}
	if err := client.Producer().ProduceSync(ctx,
		gokafka.Record{Topic: topic, Value: []byte("m1")},
		gokafka.Record{Topic: topic, Value: []byte("m2")},
		gokafka.Record{Topic: topic, Value: []byte("m3")}); err != nil {
		log.Fatalf("produce: %v", err)
	}

	// Consume one batch. Record.DeliveryCount is the KIP-932 delivery attempt
	// count (1 on first delivery, higher on redelivery) — use it for dead-letter
	// logic: Reject a record once it nears the group's delivery-count limit.
	share := client.ShareConsumer([]string{topic})
	records, err := share.Poll(ctx)
	if err != nil {
		log.Fatalf("poll: %v", err)
	}
	const deliveryLimit = 5
	for _, r := range records {
		if r.DeliveryCount >= deliveryLimit {
			log.Printf("poison %s[%d]@%d (delivered %d×) → Reject", r.Topic, r.Partition, r.Offset, r.DeliveryCount)
			_ = share.Reject(ctx, r)
			continue
		}
		log.Printf("acquired %s[%d]@%d (delivery %d): %s", r.Topic, r.Partition, r.Offset, r.DeliveryCount, r.Value)
	}
	_ = share.Leave(ctx) // implicit mode auto-accepts the batch on Leave

	// Admin: where has the group consumed to (SPSO), and how far is it behind?
	offs, err := admin.DescribeShareGroupOffsets(ctx, group)
	if err != nil {
		log.Fatalf("describe offsets: %v", err)
	}
	for _, o := range offs {
		log.Printf("SPSO %s[%d] = %d", o.Topic, o.Partition, o.StartOffset)
	}
	lag, err := admin.ShareGroupLag(ctx, group)
	if err != nil {
		log.Fatalf("lag: %v", err)
	}
	for _, l := range lag {
		log.Printf("lag %s[%d]: SPSO=%d logEnd=%d lag=%d", l.Topic, l.Partition, l.Committed, l.LogEndOffset, l.Lag)
	}

	// Reset the group to the beginning to reprocess the queue (group must be empty).
	if err := admin.AlterShareGroupOffsets(ctx, group, map[string]map[int32]int64{topic: {0: 0}}); err != nil {
		log.Printf("alter offsets: %v", err)
	}

	// Read/adjust share-group configs on the GROUP resource.
	if err := admin.AlterGroupConfigs(ctx, group, map[string]*string{
		"share.auto.offset.reset": strptr("earliest"),
	}); err != nil {
		log.Printf("alter group config: %v", err)
	}
	gc, err := admin.DescribeGroupConfigs(ctx, group)
	if err != nil {
		log.Fatalf("describe group config: %v", err)
	}
	log.Printf("group config share.auto.offset.reset = %s", gc["share.auto.offset.reset"])
}

func strptr(s string) *string { return &s }

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
