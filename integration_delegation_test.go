//go:build integration

package gokafka_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

// TestIntegrationDelegationTokens exercises the delegation-token APIs (KIP-48).
// The integration broker has no delegation.token.master.key, so Create returns
// DELEGATION_TOKEN_AUTH_DISABLED (61) — which still proves the request/response
// wire codecs round-trip end to end. Describe must not error at the wire level.
func TestIntegrationDelegationTokens(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	cli, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	admin := cli.Admin()

	_, err = admin.CreateDelegationToken(ctx, nil, nil, 0)
	if err == nil {
		t.Log("broker issued a delegation token (master key configured)")
	} else {
		var ke *gokafka.KafkaError
		if !errors.As(err, &ke) {
			t.Fatalf("expected a broker KafkaError (wire codec ok), got %T: %v", err, err)
		}
		// 61 = DELEGATION_TOKEN_AUTH_DISABLED; anything that decoded to a broker
		// error code proves the request/response framing is correct.
		t.Logf("create delegation token returned broker code %d (expected 61 when auth disabled)", ke.Code)
	}

	// Describe must round-trip cleanly (empty list when auth disabled).
	toks, err := admin.DescribeDelegationTokens(ctx)
	if err != nil {
		var ke *gokafka.KafkaError
		if !errors.As(err, &ke) {
			t.Fatalf("describe delegation tokens wire error: %v", err)
		}
	} else {
		t.Logf("describe returned %d tokens", len(toks))
	}
}
