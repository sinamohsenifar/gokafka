//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationTransactionEOS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	txnID := fmt.Sprintf("gokafka-txn-%d", time.Now().UnixNano())
	topic := fmt.Sprintf("gokafka-txn-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-txn-grp-%d", time.Now().UnixNano())

	// Setup topic
	setup, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	sclient, err := gokafka.NewClient(setup)
	if err != nil {
		t.Fatal(err)
	}
	if err := sclient.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, sclient.Admin(), topic)
	t.Cleanup(func() {
		_ = sclient.Admin().DeleteTopics(context.Background(), topic)
		sclient.Close()
	})

	// Transactional produce
	pcfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithTransaction(gokafka.TransactionConfig{Enabled: true, TransactionalID: txnID}),
	)
	if err != nil {
		t.Fatal(err)
	}
	pclient, err := gokafka.NewClient(pcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pclient.Close()

	txn, err := pclient.Producer().BeginTransaction(ctx)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("txn-record")
	if err := txn.ProduceWithinTxn(ctx, gokafka.Record{Topic: topic, Value: payload}); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	// read_committed consumer
	ccfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithConsumerGroup(group),
		gokafka.WithConsumeFromBeginning(true),
		gokafka.WithConsumer(gokafka.ConsumerConfig{IsolationLevel: gokafka.IsolationReadCommitted}),
	)
	if err != nil {
		t.Fatal(err)
	}
	cclient, err := gokafka.NewClient(ccfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cclient.Close()

	consumer := cclient.Consumer([]string{topic})
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		recs, err := consumer.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if string(r.Value) == string(payload) {
				return
			}
		}
	}
	t.Fatal("transactional record not consumed")
}
