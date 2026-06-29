//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationDeleteRecords(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cfg, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	admin := client.Admin()
	topic := fmt.Sprintf("gokafka-delrec-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, admin, topic, 1)
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic) })

	prod := client.Producer()
	for i := 0; i < 5; i++ {
		if err := prod.ProduceSync(ctx, gokafka.Record{Topic: topic, Partition: 0, Value: []byte(fmt.Sprintf("v%d", i))}); err != nil {
			t.Fatal(err)
		}
	}

	// Delete records before offset 3 → new low watermark should be 3.
	results, err := admin.DeleteRecords(ctx, map[string]map[int32]int64{topic: {0: 3}})
	if err != nil {
		t.Fatalf("delete records: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Err != nil {
		t.Fatalf("partition error: %v", results[0].Err)
	}
	if results[0].LowWatermark != 3 {
		t.Fatalf("expected low watermark 3, got %d", results[0].LowWatermark)
	}
}

func TestIntegrationElectLeaders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	cfg, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	admin := client.Admin()
	topic := fmt.Sprintf("gokafka-elect-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, admin, topic, 1)
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic) })

	// On a healthy single-broker cluster the preferred leader is already elected,
	// so the broker reports ELECTION_NOT_NEEDED (code 84) per partition. The call
	// must succeed at the protocol level and return a per-partition result.
	results, err := admin.ElectLeaders(ctx, gokafka.ElectionPreferred, map[string][]int32{topic: {0}})
	if err != nil {
		t.Fatalf("elect leaders: %v", err)
	}
	if len(results) != 1 || results[0].Topic != topic || results[0].Partition != 0 {
		t.Fatalf("unexpected results: %+v", results)
	}
	// results[0].Err may be ELECTION_NOT_NEEDED, which is acceptable here.
}
