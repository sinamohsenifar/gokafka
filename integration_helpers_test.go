//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

// skipIfUnsupportedAPI skips the test when the broker does not advertise the API
// the call needed (e.g. ElectLeaders or delegation tokens on Redpanda). GoKafka
// surfaces this as a clear "broker does not support API key" error.
func skipIfUnsupportedAPI(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), "broker does not support API") {
		t.Skipf("broker does not support this API: %v", err)
	}
}

func integrationWaitTopicReady(t *testing.T, admin *gokafka.Admin, topic string) {
	integrationWaitPartitions(t, admin, topic, 1)
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
	addr := os.Getenv(key)
	if addr == "" {
		addr = fallback
	}
	if addr == "" {
		t.Skipf("%s not set", key)
	}
	// These are optional listeners (TLS / SASL). Skip when the listener isn't
	// reachable — e.g. the Redpanda CI lane has no dedicated SSL/SASL listeners,
	// so the security tests skip there but still run on the Kafka lane.
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Skipf("%s listener %s not reachable: %v", key, addr, err)
	}
	_ = conn.Close()
	return addr
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
	integrationWaitTopicReady(t, admin, topic)
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
