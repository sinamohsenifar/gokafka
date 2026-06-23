//go:build integration

package gokafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationProduceConsume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithClientID("gokafka-integration"),
	)
	if err != nil {
		t.Fatal(err)
	}
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topic := "gokafka-it-" + time.Now().Format("150405")
	admin := client.Admin()
	if err := admin.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, admin, topic)
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic) })

	payload := []byte("integration-test")
	results, err := client.Producer().ProduceSyncResult(ctx, gokafka.Record{Topic: topic, Value: payload})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 || results[0].Offset < 0 {
		t.Fatalf("results=%+v", results)
	}

	group := "gokafka-it-" + time.Now().Format("150405")
	cfg2, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithClientID("gokafka-integration-consumer"),
		gokafka.WithConsumerGroup(group),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	cclient, err := gokafka.NewClient(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer cclient.Close()

	consumer := cclient.Consumer([]string{topic})
	deadline := time.Now().Add(20 * time.Second)
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
	t.Fatal("did not consume produced record")
}

func TestIntegrationAdminDescribeGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	groups, err := admin.ListConsumerGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) == 0 {
		return
	}
	desc, err := admin.DescribeConsumerGroups(ctx, groups[0].GroupID)
	if err != nil {
		t.Fatal(err)
	}
	if desc[0].GroupID == "" {
		t.Fatal("empty group id")
	}
}

func TestIntegrationNegotiatedVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	vers := client.NegotiatedAPIVersions()
	if len(vers) == 0 {
		t.Fatal("expected negotiated versions")
	}
	if v, ok := client.NegotiatedAPIVersion(0); !ok || v <= 0 { // APIProduce
		t.Fatalf("produce version=%d ok=%v", v, ok)
	}
	ok, err := client.SupportsAPI(ctx, 0, 3)
	if err != nil || !ok {
		t.Fatalf("SupportsAPI produce: ok=%v err=%v", ok, err)
	}
}

func TestIntegrationDescribeCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	desc, err := client.Admin().DescribeCluster(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(desc.Brokers) == 0 {
		t.Fatal("expected brokers")
	}
	if desc.ControllerID <= 0 {
		t.Fatalf("controller=%d", desc.ControllerID)
	}
}

func TestIntegrationConsumerPauseResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := gokafka.NewConfig(integrationBrokers(t),
		gokafka.WithConsumerGroup("gokafka-pause-"+time.Now().Format("150405")),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topic := "gokafka-pause-" + time.Now().Format("150405")
	admin := client.Admin()
	if err := admin.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, admin, topic)
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic) })

	if err := client.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte("first")}); err != nil {
		t.Fatal(err)
	}

	consumer := client.Consumer([]string{topic})
	recs, err := consumer.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 || string(recs[0].Value) != "first" {
		t.Fatalf("recs=%+v", recs)
	}

	if err := client.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	consumer.Pause(gokafka.TopicPartition{Topic: topic, Partition: 0})

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		recs, err = consumer.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if string(r.Value) == "second" {
				t.Fatal("received record while paused")
			}
		}
		time.Sleep(200 * time.Millisecond)
	}

	consumer.Resume(gokafka.TopicPartition{Topic: topic, Partition: 0})
	resumeDeadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(resumeDeadline) {
		recs, err = consumer.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if string(r.Value) == "second" {
				return
			}
		}
	}
	t.Fatal("expected second record after resume")
}
