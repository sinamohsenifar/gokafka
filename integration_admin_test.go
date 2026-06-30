//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationAdminTopicLifecycle(t *testing.T) {
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
	topic := fmt.Sprintf("gokafka-admin-%d", time.Now().UnixNano())

	if err := admin.CreateTopics(ctx, gokafka.TopicSpec{
		Name: topic, Partitions: 3, ReplicationFactor: 1,
		Configs: map[string]string{"cleanup.policy": "delete"},
	}); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, admin, topic, 3)
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic) })

	desc, err := admin.DescribeTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(desc.Partitions) != 3 {
		t.Fatalf("partitions=%d", len(desc.Partitions))
	}

	n, err := admin.TopicPartitions(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("partition count=%d", n)
	}

	cluster, err := admin.DescribeCluster(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cluster.ControllerID < 0 {
		t.Fatalf("controller=%d", cluster.ControllerID)
	}

	cfgs, err := admin.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfgs[topic]) == 0 {
		t.Fatal("expected topic configs")
	}

	topic2 := fmt.Sprintf("gokafka-admin-parts-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(ctx, topic2, 2, 1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic2) })
	if err := admin.CreatePartitions(ctx, topic2, 4); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, admin, topic2, 4)
}

func TestIntegrationAdminACL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	topic := fmt.Sprintf("gokafka-acl-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, admin, topic)
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic) })

	principal := "User:gokafka"
	if err := admin.CreateACLs(ctx, gokafka.ACLBinding{
		ResourceType: gokafka.ACLResourceTopic,
		ResourceName: topic,
		Principal:    principal,
		Host:         "*",
		Operation:    gokafka.ACLOperationRead,
		Permission:   gokafka.ACLPermissionAllow,
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.DeleteACLs(context.Background(), gokafka.ACLResourceTopic, topic, principal)
	})

	bindings, err := admin.DescribeACLs(ctx, gokafka.ACLResourceTopic, topic, principal)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) == 0 {
		t.Fatal("expected acl bindings from DescribeACLs")
	}
	found := false
	for _, b := range bindings {
		if b.Principal == principal && b.Operation == gokafka.ACLOperationRead {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("acl not found in describe: %+v", bindings)
	}
}

func TestIntegrationDeleteConsumerGroupOffsets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	brokers := integrationBrokers(t)
	topic := fmt.Sprintf("gokafka-offdel-%d", time.Now().UnixNano())
	group := fmt.Sprintf("gokafka-offdel-grp-%d", time.Now().UnixNano())

	setup, err := gokafka.NewConfig(brokers)
	if err != nil {
		t.Fatal(err)
	}
	client, err := gokafka.NewClient(setup)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Admin().CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitTopicReady(t, client.Admin(), topic)
	t.Cleanup(func() {
		_ = client.Admin().DeleteTopics(context.Background(), topic)
		client.Close()
	})

	if err := client.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}

	cfg, err := gokafka.NewConfig(brokers,
		gokafka.WithConsumerGroup(group),
		gokafka.WithConsumeFromBeginning(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	cclient, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cclient.Close()

	consumer := cclient.Consumer([]string{topic})
	recs, err := consumer.Poll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) == 0 {
		t.Fatal("expected a record to commit")
	}
	if err := consumer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Leave(ctx); err != nil {
		t.Fatal(err)
	}
	cclient.Close()
	time.Sleep(200 * time.Millisecond)

	admin := client.Admin()
	if err := admin.DeleteConsumerGroupOffsets(ctx, group, map[string][]int32{topic: {0}}); err != nil {
		t.Fatal(err)
	}
}
