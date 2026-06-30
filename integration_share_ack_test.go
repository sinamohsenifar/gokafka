//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"strings"
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

// TestIntegrationAdminDescribeShareGroupOffsets verifies the DescribeShareGroupOffsets
// (API 90) wire codec round-trips against a real broker: it consumes + acknowledges
// a record (advancing the share-partition start offset) then reads the offsets back.
func TestIntegrationAdminDescribeShareGroupOffsets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	brokers := integrationBrokers(t)

	probeCfg, _ := gokafka.NewConfig(brokers)
	probe, err := gokafka.NewClient(probeCfg)
	if err != nil {
		t.Fatal(err)
	}
	supported, _ := probe.SupportsAPI(ctx, protocol.APIDescribeShareGroupOffsets, 0)
	probe.Close()
	if !supported {
		t.Skip("broker does not advertise DescribeShareGroupOffsets (API 90)")
	}

	topic := fmt.Sprintf("gokafka-sgo-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-sgo-grp-%d", time.Now().UnixNano())

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

	cfg, _ := gokafka.NewConfig(brokers, gokafka.WithShareGroup(group), gokafka.WithConsumeFromBeginning(true))
	c, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	share := c.ShareConsumer([]string{topic})
	recs := pollUntil(t, ctx, share, 1)
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if err := share.Acknowledge(ctx, recs...); err != nil {
		t.Fatalf("ack: %v", err)
	}
	_ = share.Leave(ctx)

	// Primary check: the RPC round-trips (encode/decode against a real broker).
	offs, err := c.Admin().DescribeShareGroupOffsets(ctx, group)
	if err != nil {
		t.Fatalf("DescribeShareGroupOffsets: %v", err)
	}
	for _, o := range offs {
		if o.Topic == topic && o.Partition == 0 {
			if o.ErrorCode != 0 {
				t.Fatalf("partition %s-0 error %d (%s)", topic, o.ErrorCode, o.ErrorMessage)
			}
			if o.StartOffset < 0 {
				t.Fatalf("unexpected StartOffset %d for %s-0", o.StartOffset, topic)
			}
		}
	}
}

// TestIntegrationAdminAlterDeleteShareGroupOffsets verifies the AlterShareGroupOffsets
// (API 91) and DeleteShareGroupOffsets (API 92) wire codecs round-trip against a
// real broker: consume+ack+leave (group becomes empty), reset the SPSO, confirm
// via Describe, then delete.
func TestIntegrationAdminAlterDeleteShareGroupOffsets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	brokers := integrationBrokers(t)

	probeCfg, _ := gokafka.NewConfig(brokers)
	probe, err := gokafka.NewClient(probeCfg)
	if err != nil {
		t.Fatal(err)
	}
	supported, _ := probe.SupportsAPI(ctx, protocol.APIAlterShareGroupOffsets, 0)
	probe.Close()
	if !supported {
		t.Skip("broker does not advertise AlterShareGroupOffsets (API 91)")
	}

	topic := fmt.Sprintf("gokafka-aso-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-aso-grp-%d", time.Now().UnixNano())

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

	cfg, _ := gokafka.NewConfig(brokers, gokafka.WithShareGroup(group), gokafka.WithConsumeFromBeginning(true))
	c, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Producer().ProduceSync(ctx,
		gokafka.Record{Topic: topic, Value: []byte("x")},
		gokafka.Record{Topic: topic, Value: []byte("y")}); err != nil {
		t.Fatal(err)
	}
	share := c.ShareConsumer([]string{topic})
	recs := pollUntil(t, ctx, share, 1)
	if len(recs) < 1 {
		t.Fatalf("expected >=1 record, got %d", len(recs))
	}
	_ = share.Acknowledge(ctx, recs...)
	if err := share.Leave(ctx); err != nil {
		t.Fatalf("leave: %v", err)
	}

	// Alter/Delete require an empty group; the member may take a moment to drain
	// after Leave, so retry briefly.
	var alterErr error
	for i := 0; i < 12; i++ {
		alterErr = c.Admin().AlterShareGroupOffsets(ctx, group, map[string]map[int32]int64{topic: {0: 0}})
		if alterErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if alterErr != nil {
		t.Fatalf("AlterShareGroupOffsets: %v", alterErr)
	}

	offs, err := c.Admin().DescribeShareGroupOffsets(ctx, group)
	if err != nil {
		t.Fatalf("describe after alter: %v", err)
	}
	for _, o := range offs {
		if o.Topic == topic && o.Partition == 0 && o.StartOffset != 0 {
			t.Fatalf("after alter to 0, StartOffset = %d (want 0)", o.StartOffset)
		}
	}

	if err := c.Admin().DeleteShareGroupOffsets(ctx, group, topic); err != nil {
		t.Fatalf("DeleteShareGroupOffsets: %v", err)
	}
}

// TestIntegrationAdminGroupConfigs verifies the public GROUP-config write path:
// AlterGroupConfigs sets share-group configs on the GROUP resource (type 32) and
// DescribeGroupConfigs reads them back.
func TestIntegrationAdminGroupConfigs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	brokers := integrationBrokers(t)

	probeCfg, _ := gokafka.NewConfig(brokers)
	probe, err := gokafka.NewClient(probeCfg)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := probe.NegotiatedAPIVersion(protocol.APIShareGroupHeartbeat); !ok || v == 0 {
		probe.Close()
		t.Skip("broker does not support KIP-932 / GROUP config resource (needs Kafka 4.1+)")
	}
	probe.Close()

	cfg, _ := gokafka.NewConfig(brokers)
	c, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	group := fmt.Sprintf("gokafka-grpcfg-%d", time.Now().UnixNano())
	latest, readCommitted := "latest", "read_committed"
	if err := c.Admin().AlterGroupConfigs(ctx, group, map[string]*string{
		"share.auto.offset.reset": &latest,
		"share.isolation.level":   &readCommitted,
	}); err != nil {
		t.Fatalf("AlterGroupConfigs: %v", err)
	}
	got, err := c.Admin().DescribeGroupConfigs(ctx, group)
	if err != nil {
		t.Fatalf("DescribeGroupConfigs: %v", err)
	}
	if got["share.auto.offset.reset"] != "latest" {
		t.Fatalf("share.auto.offset.reset = %q, want latest", got["share.auto.offset.reset"])
	}
	if got["share.isolation.level"] != "read_committed" {
		t.Fatalf("share.isolation.level = %q, want read_committed", got["share.isolation.level"])
	}
}

// TestIntegrationShareUnsupportedBrokerClearError: on a broker without KIP-932
// (no share.version, or Redpanda), a ShareConsumer must surface a clear
// "does not support share groups" error instead of an opaque heartbeat failure
// or connection reset deep inside Poll.
func TestIntegrationShareUnsupportedBrokerClearError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	brokers := integrationBrokers(t)

	probeCfg, _ := gokafka.NewConfig(brokers)
	probe, err := gokafka.NewClient(probeCfg)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := probe.NegotiatedAPIVersion(protocol.APIShareGroupHeartbeat); ok && v >= 1 {
		probe.Close()
		t.Skip("broker supports KIP-932; this test covers the unsupported-broker guard")
	}
	probe.Close()

	cfg, err := gokafka.NewConfig(brokers, gokafka.WithShareGroup("gokafka-unsupported-grp"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	share := c.ShareConsumer([]string{"gokafka-unsupported-topic"})
	if _, err := share.Poll(ctx); err == nil {
		t.Fatal("expected a clear unsupported-share-groups error, got nil")
	} else if !strings.Contains(err.Error(), "share group") {
		t.Fatalf("error should clearly mention share groups, got: %v", err)
	}
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

// TestIntegrationShareDeliveryCount: the KIP-932 delivery_count is surfaced on
// Record and increments on redelivery after a Release (1 on first delivery, 2
// after the record is released and re-acquired).
func TestIntegrationShareDeliveryCount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	c, topic := shareEnv(t, ctx, []string{"d1"})
	share := c.ShareConsumer([]string{topic})

	first := pollUntil(t, ctx, share, 1)
	if len(first) != 1 {
		t.Fatalf("expected 1 record, got %d", len(first))
	}
	if first[0].DeliveryCount != 1 {
		t.Fatalf("first delivery DeliveryCount = %d, want 1", first[0].DeliveryCount)
	}
	if err := share.Release(ctx, first...); err != nil {
		t.Fatalf("release: %v", err)
	}

	short, cancel2 := context.WithTimeout(ctx, 8*time.Second)
	defer cancel2()
	again := pollUntil(t, short, share, 1)
	if len(again) != 1 {
		t.Fatalf("released record should be redelivered, got %d", len(again))
	}
	if again[0].DeliveryCount != 2 {
		t.Fatalf("redelivered DeliveryCount = %d, want 2", again[0].DeliveryCount)
	}
	_ = share.Acknowledge(ctx, again...)
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

// TestIntegrationShareImplicitAutoAccept: in implicit mode a delivered batch is
// auto-accepted with no manual Acknowledge, so a fresh consumer sees no
// redelivery. The auto-accept fires both at the start of the next Poll and on
// Leave via the same code path; this exercises the deterministic Leave path.
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
	// Leave auto-accepts the delivered batch (implicit mode) — no manual Acknowledge.
	if err := share.Leave(ctx); err != nil {
		t.Fatalf("leave: %v", err)
	}

	// A fresh consumer on the same group must not see the auto-accepted records.
	fresh := c.ShareConsumer([]string{topic})
	short, cancel2 := context.WithTimeout(ctx, 6*time.Second)
	defer cancel2()
	leftover := pollUntil(t, short, fresh, 1)
	if len(leftover) != 0 {
		t.Fatalf("implicit-accepted records must not be redelivered, got %v", leftover)
	}
	_ = fresh.Leave(ctx)
}
