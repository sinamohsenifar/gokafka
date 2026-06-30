//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
)

func TestIntegrationAlterUserScramCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	adminCfg, err := gokafka.NewConfig(integrationBrokers(t))
	if err != nil {
		t.Fatal(err)
	}
	adminClient, err := gokafka.NewClient(adminCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()
	admin := adminClient.Admin()

	user := fmt.Sprintf("gokafka-scram-%d", time.Now().UnixNano())
	pass := "s3cr3t-pw"
	if err := admin.UpsertUserScramCredential(ctx, user, gokafka.ScramSHA256, pass, 4096); err != nil {
		t.Fatalf("upsert scram credential: %v", err)
	}
	t.Cleanup(func() {
		_ = admin.DeleteUserScramCredential(context.Background(), user, gokafka.ScramSHA256)
	})

	// DescribeUserScramCredentials should report the credential we just upserted.
	descs, err := admin.DescribeUserScramCredentials(ctx, user)
	if err != nil {
		t.Fatalf("describe scram credentials: %v", err)
	}
	var described bool
	for _, d := range descs {
		if d.User != user {
			continue
		}
		for _, c := range d.Credentials {
			if c.Mechanism == gokafka.ScramSHA256 && c.Iterations == 4096 {
				described = true
			}
		}
	}
	if !described {
		t.Fatalf("upserted SCRAM credential not found in describe: %+v", descs)
	}

	// Verify the credential works by authenticating with it on the SASL listener.
	// This requires a SASL_PLAINTEXT listener on the SAME cluster the credential
	// was created on; skip when that isn't explicitly provided (e.g. the Redpanda
	// CI lane, which has no separate SASL listener configured here).
	if os.Getenv("KAFKA_BROKERS_SASL_PLAINTEXT") == "" {
		t.Skip("KAFKA_BROKERS_SASL_PLAINTEXT not set; skipping SCRAM auth round-trip (upsert+describe already verified)")
	}
	saslBrokers := integrationBrokerEnv(t, "KAFKA_BROKERS_SASL_PLAINTEXT", "127.0.0.1:9094")
	authCfg, err := gokafka.NewConfig([]string{saslBrokers}, gokafka.WithSecurity(gokafka.SecurityConfig{
		Protocol: gokafka.SecuritySASLPlaintext,
		SASL:     gokafka.SASLConfig{Mechanism: gokafka.SASLSCRAM256, Username: user, Password: pass},
	}))
	if err != nil {
		t.Fatal(err)
	}
	var authErr error
	for i := 0; i < 10; i++ {
		ac, err := gokafka.NewClient(authCfg)
		if err == nil {
			_, authErr = ac.Admin().ListTopics(ctx)
			ac.Close()
			if authErr == nil {
				break
			}
		} else {
			authErr = err
		}
		time.Sleep(500 * time.Millisecond)
	}
	if authErr != nil {
		t.Fatalf("authentication with upserted SCRAM credential failed: %v", authErr)
	}
}

func TestIntegrationDescribeLogDirs(t *testing.T) {
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

	topic := fmt.Sprintf("gokafka-logdirs-%d", time.Now().UnixNano())
	if err := admin.CreateTopic(ctx, topic, 1, 1); err != nil {
		t.Fatal(err)
	}
	integrationWaitPartitions(t, admin, topic, 1)
	t.Cleanup(func() { _ = admin.DeleteTopics(context.Background(), topic) })
	if err := client.Producer().ProduceSync(ctx, gokafka.Record{Topic: topic, Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}

	dirs, err := admin.DescribeLogDirs(ctx, nil)
	if err != nil {
		t.Fatalf("describe log dirs: %v", err)
	}
	if len(dirs) == 0 {
		t.Fatal("expected at least one log dir")
	}
	found := false
	for _, d := range dirs {
		if d.Err != nil {
			continue
		}
		for _, p := range d.Partitions {
			if p.Topic == topic && p.Partition == 0 && p.Size >= 0 {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("topic %s partition not found in any log dir", topic)
	}
}
