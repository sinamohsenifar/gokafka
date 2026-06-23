package gokafka_test

import (
	"testing"

	"github.com/sinamohsenifar/gokafka"
)

func TestHashPartitioner(t *testing.T) {
	p := gokafka.HashPartitioner{}
	if got := p.Partition([]byte("key"), 3); got < 0 || got >= 3 {
		t.Fatalf("partition out of range: %d", got)
	}
	if p.Partition([]byte("key"), 3) != p.Partition([]byte("key"), 3) {
		t.Fatal("expected stable hash partition")
	}
}

func TestRoundRobinPartitioner(t *testing.T) {
	p := &gokafka.RoundRobinPartitioner{}
	seen := map[int32]bool{}
	for i := 0; i < 6; i++ {
		seen[p.Partition(nil, 3)] = true
	}
	if len(seen) != 3 {
		t.Fatalf("expected all partitions visited, got %v", seen)
	}
}

func TestWithConsumerMergesConfig(t *testing.T) {
	cfg, err := gokafka.NewConfig([]string{"localhost:9092"},
		gokafka.WithConsumeFromBeginning(true),
		gokafka.WithConsumer(gokafka.ConsumerConfig{IsolationLevel: gokafka.IsolationReadCommitted}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Consumer.ConsumeFromBeginning {
		t.Fatal("ConsumeFromBeginning should survive WithConsumer merge")
	}
	if cfg.Consumer.IsolationLevel != gokafka.IsolationReadCommitted {
		t.Fatal("expected read_committed isolation")
	}
}

func TestNewConfigValidation(t *testing.T) {
	if _, err := gokafka.NewConfig(nil); err == nil {
		t.Fatal("expected error for empty brokers")
	}
	cfg, err := gokafka.NewConfig([]string{"localhost:9092"},
		gokafka.WithConsumerGroup("app"),
		gokafka.WithTransaction(gokafka.TransactionConfig{Enabled: true, TransactionalID: "txn-1"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConsumerGroup != "app" {
		t.Fatalf("group=%q", cfg.ConsumerGroup)
	}
}

func TestIdempotentRequiresAcksAll(t *testing.T) {
	_, err := gokafka.NewConfig([]string{"localhost:9092"},
		gokafka.WithProducer(gokafka.ProducerConfig{Idempotent: true, Acks: gokafka.AcksOne}),
	)
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestWithAutoCommit(t *testing.T) {
	cfg, err := gokafka.NewConfig([]string{"localhost:9092"}, gokafka.WithAutoCommit(true))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Consumer.AutoCommit {
		t.Fatal("expected auto commit enabled")
	}
}

func TestZstdCompressionRoundTrip(t *testing.T) {
	cfg, err := gokafka.NewConfig([]string{"localhost:9092"},
		gokafka.WithProducer(gokafka.ProducerConfig{
			Compression: gokafka.CompressionZstd,
			Acks:        gokafka.AcksAll,
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Producer.Compression != gokafka.CompressionZstd {
		t.Fatal("expected zstd compression")
	}
}

func TestDataTypes(t *testing.T) {
	b, err := gokafka.BytesPayload([]byte("x")).Encode()
	if err != nil || string(b) != "x" {
		t.Fatal(b, err)
	}
	b, err = gokafka.StringPayload("hi").Encode()
	if err != nil || string(b) != "hi" {
		t.Fatal(b, err)
	}
	b, err = gokafka.JSONPayload{V: map[string]int{"n": 1}}.Encode()
	if err != nil || len(b) == 0 {
		t.Fatal(b, err)
	}
}

func TestConnectionHostRemap(t *testing.T) {
	cfg := gokafka.ConnectionConfig{
		HostRemap: map[string]string{"kafka:29092": "localhost:9092"},
	}
	addr := cfg.ResolveBrokerAddress(1, "kafka", 29092)
	if addr != "localhost:9092" {
		t.Fatalf("addr=%q", addr)
	}
}

func TestIsRetriable(t *testing.T) {
	if !gokafka.IsRetriable(&gokafka.KafkaError{Code: gokafka.ErrCodeNotLeaderForPart}) {
		t.Fatal("expected retriable")
	}
	if gokafka.IsRetriable(gokafka.ErrClosed) {
		t.Fatal("expected non-retriable")
	}
}
