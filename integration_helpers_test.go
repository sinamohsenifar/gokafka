//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func integrationWaitTopicReady() {
	time.Sleep(300 * time.Millisecond)
}

func integrationWaitPartitions(t *testing.T, admin *gokafka.Admin, topic string, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		n, err := admin.TopicPartitions(ctx, topic)
		if err == nil && n == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("topic %s partitions=%d want=%d err=%v", topic, n, want, err)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func integrationBrokers(t *testing.T) []string {
	t.Helper()
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		t.Skip("KAFKA_BROKERS not set; run docker compose up and export KAFKA_BROKERS=127.0.0.1:9092")
	}
	return []string{b}
}

func integrationBrokerEnv(t *testing.T, key, fallback string) string {
	t.Helper()
	if v := os.Getenv(key); v != "" {
		return v
	}
	if fallback != "" {
		return fallback
	}
	t.Skipf("%s not set", key)
	return ""
}

func secretsDir(t *testing.T) string {
	t.Helper()
	root := os.Getenv("GOKAFKA_SECRETS_DIR")
	if root == "" {
		// Relative to module root when tests run from repo root.
		root = filepath.Join("docker", "secrets")
	}
	if _, err := os.Stat(filepath.Join(root, "ca.crt")); err != nil {
		t.Skipf("TLS secrets not found at %s (run scripts/gen-test-certs.sh)", root)
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		t.Fatal(err)
	}
	return abs
}

func testTLSConfig(t *testing.T) gokafka.TLSConfig {
	t.Helper()
	dir := secretsDir(t)
	return gokafka.TLSConfig{
		CAFile:             filepath.Join(dir, "ca.crt"),
		InsecureSkipVerify: true,
		ServerName:         "localhost",
	}
}

func testMTLSConfig(t *testing.T) gokafka.TLSConfig {
	t.Helper()
	dir := secretsDir(t)
	cfg := testTLSConfig(t)
	cfg.CertFile = filepath.Join(dir, "client.crt")
	cfg.KeyFile = filepath.Join(dir, "client.key")
	return cfg
}

func integrationProduceConsume(t *testing.T, brokers []string, opts ...gokafka.Option) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	base := []gokafka.Option{gokafka.WithClientID("gokafka-it-security")}
	base = append(base, opts...)

	cfg, err := gokafka.NewConfig(brokers, base...)
	if err != nil {
		t.Fatal(err)
	}
	client, err := gokafka.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	topic := fmt.Sprintf("gokafka-sec-%d", time.Now().UnixNano())
	admin := client.Admin()
	if err := admin.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic) })

	payload := []byte("security-integration")
	if err := client.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: payload}); err != nil {
		t.Fatal(err)
	}

	consumerOpts := append([]gokafka.Option{}, base...)
	consumerOpts = append(consumerOpts,
		gokafka.WithConsumerGroup("gokafka-sec-"+time.Now().Format("150405.000")),
		gokafka.WithConsumeFromBeginning(true),
	)
	cfg2, err := gokafka.NewConfig(brokers, consumerOpts...)
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
