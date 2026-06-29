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

// TestIntegrationConsumerPatternRegex verifies KIP-848 server-side RE2J regex
// subscriptions: ConsumerPattern subscribes to all topics matching a pattern,
// and the broker assigns only the matching ones.
func TestIntegrationConsumerPatternRegex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	probeCfg, _ := gokafka.NewConfig(integrationBrokers(t))
	probe, err := gokafka.NewClient(probeCfg)
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := probe.NegotiatedAPIVersion(protocol.APIConsumerGroupHeartbeat); !ok || v == 0 {
		probe.Close()
		t.Skip("broker does not support KIP-848 ConsumerGroupHeartbeat")
	}
	probe.Close()

	run := time.Now().UnixNano()
	prefix := fmt.Sprintf("gokafka-rx%d", run)
	matchA := prefix + "-alpha"
	matchB := prefix + "-beta"
	noMatch := fmt.Sprintf("gokafka-nomatch%d", run)
	pattern := "^" + prefix + "-.*$"

	pcfg, _ := gokafka.NewConfig(integrationBrokers(t))
	pcli, err := gokafka.NewClient(pcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pcli.Close()
	for _, tp := range []string{matchA, matchB, noMatch} {
		if err := pcli.Admin().CreateTopic(ctx, tp, 1, 1); err != nil {
			t.Fatalf("create %s: %v", tp, err)
		}
		integrationWaitPartitions(t, pcli.Admin(), tp, 1)
		if err := pcli.Producer().ProduceSync(ctx, gokafka.Record{Topic: tp, Value: []byte(tp)}); err != nil {
			t.Fatalf("produce %s: %v", tp, err)
		}
	}
	t.Cleanup(func() { _ = pcli.Admin().DeleteTopics(context.Background(), matchA, matchB, noMatch) })

	ccfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithConsumerGroup(fmt.Sprintf("%s-grp", prefix)),
		gokafka.WithGroupProtocol(gokafka.GroupProtocolNextGen),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	ccli, err := gokafka.NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ccli.Close()
	cons := ccli.ConsumerPattern(pattern)

	seen := map[string]bool{}
	deadline := time.Now().Add(40 * time.Second)
	for !(seen[matchA] && seen[matchB]) && time.Now().Before(deadline) {
		recs, err := cons.Poll(ctx)
		if err != nil {
			t.Fatalf("poll: %v", err)
		}
		for _, r := range recs {
			seen[string(r.Value)] = true
		}
	}
	if !seen[matchA] || !seen[matchB] {
		t.Fatalf("regex consumer should see matching topics, seen=%v", seen)
	}
	if seen[noMatch] {
		t.Fatalf("regex consumer must NOT see non-matching topic %q", noMatch)
	}
}
