//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// shareEnv spins up a single-partition topic on a share-capable broker, produces
// the given values, and returns a client configured for the share group. Skips
// when the broker doesn't support KIP-932.
func shareEnv(t *testing.T, ctx context.Context, values []string, opts ...gokafka.Option) (*gokafka.Client, string) {
	t.Helper()
	brokers := integrationBrokers(t)

	probeCfg, _ := gokafka.NewConfig(brokers)
	probe, err := gokafka.NewClient(probeCfg)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := probe.NegotiatedAPIVersion(protocol.APIShareGroupHeartbeat); !ok || v == 0 {
		probe.Close()
		t.Skip("broker does not support KIP-932 ShareGroupHeartbeat (Kafka 4.1+ with share.version=1)")
	}
	probe.Close()

	topic := fmt.Sprintf("gokafka-shareack-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-shareack-grp-%d", time.Now().UnixNano())

	adminCfg, _ := gokafka.NewConfig(brokers)
	admin, err := gokafka.NewClient(adminCfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, admin.Admin(), topic, 1)
	t.Cleanup(func() {
		_ = admin.Admin().DeleteTopics(context.Background(), topic)
		admin.Close()
	})

	base := append([]gokafka.Option{gokafka.WithShareGroup(group), gokafka.WithConsumeFromBeginning(true)}, opts...)
	cfg, err := gokafka.NewConfig(brokers, base...)
	if err != nil {
		t.Fatal(err)
	}
	c, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	recs := make([]gokafka.Record, len(values))
	for i, v := range values {
		recs[i] = gokafka.Record{Topic: topic, Value: []byte(v)}
	}
	if err := c.Producer().ProduceSync(ctx, recs...); err != nil {
		t.Fatal(err)
	}
	return c, topic
}

// pollUntil polls until it gathers at least want records or the (short) ctx ends.
func pollUntil(t *testing.T, ctx context.Context, share *gokafka.ShareConsumer, want int) []gokafka.Record {
	t.Helper()
	var got []gokafka.Record
	for len(got) < want {
		recs, err := share.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			t.Fatalf("poll: %v", err)
		}
		got = append(got, recs...)
		if ctx.Err() != nil {
			break
		}
	}
	return got
}

// TestIntegrationShareReleaseRedelivers: a Released record returns to the group
// and is redelivered on a later poll.
func TestIntegrationShareReleaseRedelivers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	c, topic := shareEnv(t, ctx, []string{"r1"})
	share := c.ShareConsumer([]string{topic})

	first := pollUntil(t, ctx, share, 1)
	if len(first) != 1 {
		t.Fatalf("expected 1 acquired record, got %d", len(first))
	}
	if err := share.Release(ctx, first...); err != nil {
		t.Fatalf("release: %v", err)
	}

	short, cancel2 := context.WithTimeout(ctx, 8*time.Second)
	defer cancel2()
	redelivered := pollUntil(t, short, share, 1)
	if len(redelivered) != 1 || string(redelivered[0].Value) != "r1" {
		t.Fatalf("released record should be redelivered, got %v", redelivered)
	}
	_ = share.Acknowledge(ctx, redelivered...)
	_ = share.Leave(ctx)
}

// TestIntegrationShareRejectNoRedelivery: a Rejected record is archived and not
// redelivered.
func TestIntegrationShareRejectNoRedelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	c, topic := shareEnv(t, ctx, []string{"j1"})
	share := c.ShareConsumer([]string{topic})

	first := pollUntil(t, ctx, share, 1)
	if len(first) != 1 {
		t.Fatalf("expected 1 record, got %d", len(first))
	}
	if err := share.Reject(ctx, first...); err != nil {
		t.Fatalf("reject: %v", err)
	}

	short, cancel2 := context.WithTimeout(ctx, 6*time.Second)
	defer cancel2()
	again := pollUntil(t, short, share, 1)
	if len(again) != 0 {
		t.Fatalf("rejected record must not be redelivered, got %v", again)
	}
	_ = share.Leave(ctx)
}

// TestIntegrationShareImplicitAutoAccept: in implicit mode the previous Poll batch
// is auto-accepted on the next Poll, so a fresh consumer sees no redelivery.
func TestIntegrationShareImplicitAutoAccept(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, topic := shareEnv(t, ctx, []string{"i1", "i2"},
		gokafka.WithShareAcknowledgementMode(gokafka.ShareAckImplicit))
	share := c.ShareConsumer([]string{topic})

	first := pollUntil(t, ctx, share, 2)
	if len(first) != 2 {
		t.Fatalf("expected 2 acquired records, got %d", len(first))
	}
	// Second poll auto-accepts the first batch (no manual Acknowledge), then
	// returns empty within the short window.
	short, cancel2 := context.WithTimeout(ctx, 5*time.Second)
	defer cancel2()
	_ = pollUntil(t, short, share, 1)
	if err := share.Leave(ctx); err != nil {
		t.Fatalf("leave: %v", err)
	}

	// A fresh consumer on the same group must not see the auto-accepted records.
	fresh := c.ShareConsumer([]string{topic})
	short2, cancel3 := context.WithTimeout(ctx, 6*time.Second)
	defer cancel3()
	leftover := pollUntil(t, short2, fresh, 1)
	if len(leftover) != 0 {
		t.Fatalf("implicit-accepted records must not be redelivered, got %v", leftover)
	}
	_ = fresh.Leave(ctx)
}
