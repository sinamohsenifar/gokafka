//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

// TestIntegrationReadCommittedFiltersAborted verifies that a read_committed
// consumer sees committed records but never records from aborted transactions,
// while a read_uncommitted consumer sees both.
func TestIntegrationReadCommittedFiltersAborted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := fmt.Sprintf("gokafka-rc-%d", time.Now().UnixNano())
	txnID := fmt.Sprintf("gokafka-rc-txn-%d", time.Now().UnixNano())

	admCfg, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	adm, err := gokafka.NewClient(admCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer adm.Close()
	if err := adm.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, adm.Admin(), topic, 1)
	t.Cleanup(func() { _ = adm.Admin().DeleteTopics(context.Background(), topic) })

	pcfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithTransaction(gokafka.TransactionConfig{Enabled: true, TransactionalID: txnID}),
	)
	if err != nil {
		t.Fatal(err)
	}
	pcli, err := gokafka.NewClient(pcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pcli.Close()

	produce := func(value string, commit bool) {
		txn, err := pcli.Producer().BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("begin txn: %v", err)
		}
		if err := txn.ProduceWithinTxn(ctx, gokafka.Record{Topic: topic, Value: []byte(value)}); err != nil {
			t.Fatalf("produce within txn: %v", err)
		}
		if commit {
			if err := txn.Commit(ctx); err != nil {
				t.Fatalf("commit: %v", err)
			}
		} else {
			_ = txn.Abort(ctx)
		}
	}

	produce("committed-1", true)
	produce("aborted-1", false)
	produce("committed-2", true)

	collect := func(isolation gokafka.IsolationLevel) map[string]bool {
		cfg, err := gokafka.NewConfig(integrationBrokers(t),
			gokafka.WithConsumerGroup(fmt.Sprintf("%s-%d", txnID, time.Now().UnixNano())),
			gokafka.WithConsumeFromBeginning(true),
			gokafka.WithConsumer(gokafka.ConsumerConfig{IsolationLevel: isolation}),
		)
		if err != nil {
			t.Fatal(err)
		}
		cli, err := gokafka.NewClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer cli.Close()
		cons := cli.Consumer([]string{topic})
		seen := map[string]bool{}
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			recs, err := cons.Poll(ctx)
			if err != nil {
				t.Fatalf("poll: %v", err)
			}
			for _, r := range recs {
				seen[string(r.Value)] = true
			}
			if seen["committed-2"] {
				break // last committed record reached
			}
		}
		return seen
	}

	committed := collect(gokafka.IsolationReadCommitted)
	if !committed["committed-1"] || !committed["committed-2"] {
		t.Fatalf("read_committed missing committed records: %v", committed)
	}
	if committed["aborted-1"] {
		t.Fatalf("read_committed must NOT see aborted record, got: %v", committed)
	}

	all := collect(gokafka.IsolationReadUncommitted)
	if !all["aborted-1"] {
		t.Fatalf("read_uncommitted should see the aborted record, got: %v", all)
	}
}
