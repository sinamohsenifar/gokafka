//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationClientQuotas(t *testing.T) {
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

	user := fmt.Sprintf("gokafka-quota-%d", time.Now().UnixNano())
	entity := gokafka.QuotaEntity{gokafka.QuotaEntityUser: user}

	if err := admin.SetClientQuota(ctx, entity,
		gokafka.QuotaOp{Key: "producer_byte_rate", Value: 1048576},
		gokafka.QuotaOp{Key: "consumer_byte_rate", Value: 2097152},
	); err != nil {
		t.Fatalf("set quota: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.SetClientQuota(context.Background(), entity,
			gokafka.QuotaOp{Key: "producer_byte_rate", Remove: true},
			gokafka.QuotaOp{Key: "consumer_byte_rate", Remove: true},
		)
	})

	name := user
	// AlterClientQuotas → DescribeClientQuotas can race on KRaft metadata
	// propagation, so poll briefly for the entry to appear.
	var entries []gokafka.QuotaEntry
	for i := 0; i < 20; i++ {
		entries, err = admin.DescribeClientQuotas(ctx, []gokafka.QuotaFilterComponent{
			{EntityType: gokafka.QuotaEntityUser, MatchName: &name},
		}, true)
		if err != nil {
			t.Fatalf("describe quotas: %v", err)
		}
		if len(entries) == 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 quota entry, got %d", len(entries))
	}
	if entries[0].Entity[gokafka.QuotaEntityUser] != user {
		t.Fatalf("unexpected entity: %v", entries[0].Entity)
	}
	if got := entries[0].Values["producer_byte_rate"]; got != 1048576 {
		t.Fatalf("producer_byte_rate = %v, want 1048576", got)
	}
	if got := entries[0].Values["consumer_byte_rate"]; got != 2097152 {
		t.Fatalf("consumer_byte_rate = %v, want 2097152", got)
	}
}
