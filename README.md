# GoKafka

[![CI](https://github.com/sinamohsenifar/gokafka/actions/workflows/ci.yml/badge.svg)](https://github.com/sinamohsenifar/gokafka/actions/workflows/ci.yml)
[![Integration](https://github.com/sinamohsenifar/gokafka/actions/workflows/integration.yml/badge.svg)](https://github.com/sinamohsenifar/gokafka/actions/workflows/integration.yml)
[![Compatibility](https://github.com/sinamohsenifar/gokafka/actions/workflows/compatibility.yml/badge.svg)](https://github.com/sinamohsenifar/gokafka/actions/workflows/compatibility.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/sinamohsenifar/gokafka.svg)](https://pkg.go.dev/github.com/sinamohsenifar/gokafka)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

GoKafka is a pure Go client for Apache Kafka. It speaks the Kafka binary protocol directly using the Go standard library—no CGO, no `librdkafka`, and no third-party modules in `go.mod`.

The API is built around `context.Context`, functional options, and explicit error types. It targets Kafka **3.4+** and **KRaft** clusters, with negotiated API versions at connect time.

```bash
go get github.com/sinamohsenifar/gokafka@v0.22.0
```

**Requirements:** Go 1.22+ · Kafka 3.4+ (KRaft recommended; 4.x is KRaft-only). CI tests Go 1.22–1.24 against Kafka 3.9.2 and 4.3.0.

### Supported Apache Kafka releases

Aligned with [Apache Kafka downloads](https://kafka.apache.org/community/downloads/):

| Kafka release | Support tier | CI coverage |
|---------------|--------------|-------------|
| **3.9.2** | LTS bridge (3.x) | Primary integration (every PR) |
| **4.0.2** | Supported | Compatibility matrix |
| **4.1.2** | Supported | Scheduled compatibility matrix |
| **4.2.1** | Supported | Scheduled compatibility matrix |
| **4.3.0** | Latest | Compatibility matrix (every PR) |

See [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md) for Go + broker matrix details.

---

## How it compares

Several mature Kafka clients exist for Go. They differ mainly in dependencies, deployment model, and how much of the protocol they expose.

| | **GoKafka** | [franz-go](https://github.com/twmb/franz-go) | [segmentio/kafka-go](https://github.com/segmentio/kafka-go) | [IBM/sarama](https://github.com/IBM/sarama) | [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) |
|---|-------------|----------------------------------------------|-------------------------------------------------------------|---------------------------------------------|---------------------------------------------------------------------------|
| **Dependencies** | stdlib only | Go modules | Go modules | Go modules | CGO + librdkafka |
| **Pure Go binary** | Yes | Yes | Yes | Yes | No (native libs) |
| **Protocol implementation** | In-tree | In-tree | In-tree | In-tree | librdkafka wrapper |
| **Idempotent producer** | Yes | Yes | Yes | Yes | Yes |
| **Transactions (EOS)** | Yes | Yes | Limited | Yes | Yes |
| **Consumer groups** | Yes | Yes | Yes | Yes | Yes |
| **Cooperative rebalance** | Yes | Yes | Partial | Yes | Yes |
| **Admin client** | Yes | Yes | Partial | Yes | Yes |
| **ACL admin** | Yes | Yes | No | Yes | Yes |
| **zstd compression** | Yes (pure Go) | Yes | Yes | Yes | Yes |
| **GSSAPI (SPNEGO pass-through)** | Yes | Yes | No | Yes | Yes |
| **Kerberos (GSSAPI)** | SPNEGO pass-through | Yes | No | Yes | Yes |
| **Schema Registry client** | Yes (REST + wire) | Via plugins | No | No | Via schema registry client |

**When GoKafka fits well**

- You want a **single static binary** with no native Kafka libraries.
- You want **no third-party modules** in `go.mod`.
- You need producer, consumer, admin, security, and observability in one module without pulling in a large dependency tree.

**When to consider alternatives**

- You need **full in-process Kerberos/KDC** today—use franz-go or confluent-kafka-go (GoKafka supports SPNEGO token pass-through only).
- You already standardize on **librdkafka** (Confluent platform, existing ops tooling)—confluent-kafka-go is the natural choice.
- You want a minimal consumer-only library with a smaller API surface—segmentio/kafka-go is lightweight for read-heavy workloads.

GoKafka covers idempotent produce, transactions, consumer groups, admin, TLS/SASL, and Schema Registry helpers in one module.

---

## Features

### Producer

- Synchronous produce with per-record **topic, partition, and offset** in the result
- Async producer with worker pool and delivery channel
- Batch producer with linger and batch-size tuning
- **Idempotent produce** enabled by default (`acks=all`, sequence reservation on retry)
- **Transactional produce** (`BeginTransaction`, `ProduceWithinTxn`, `Commit` / `Abort`)
- Partitioners: hash (key-based) and round-robin
- Compression: none, gzip, snappy, lz4, **zstd** (pure Go, stdlib-only)
- Record headers and timestamps

### Consumer

- Consumer groups with partition assignors: range, round-robin, sticky, cooperative-sticky
- `Poll` loop or `Run` with handler and optional worker pool
- **Commit after successful processing** (`Consumer.Run`); auto-commit is opt-in
- Static group membership (`group.instance.id`)
- `read_committed` isolation for transactional topics
- Offset management: committed offsets, `Seek`, `SeekToBeginning`, `SeekToEnd`
- Pause and resume per partition
- Rebalance callbacks (`RebalanceListener`)

### Transactions

- Full consume-transform-produce flow: `SendOffsetsToTxn`, `ProduceWithinTxn`, `Commit`
- Transaction coordinator discovery via `FindCoordinator`
- Partition registration (`AddPartitionsToTxn`) before transactional produce

### Admin

- Topics: create (with configs), delete, describe, list
- Partitions: create, describe leaders/ISR/replicas
- Configs: describe, alter, incremental alter
- Consumer groups: list, describe, delete
- Offsets: delete committed offsets for a group
- Cluster metadata: brokers, controller, cluster id
- ACLs: create, describe, delete

### Security

- `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`
- SASL: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, OAUTHBEARER (wire protocol)
- TLS: CA trust, client certificates (mTLS), SNI
- Advertised listener remapping for Docker and Kubernetes (`WithBrokerHostRemap`)

### Schema Registry

- Confluent Schema Registry REST client (register, lookup, compatibility)
- Confluent wire-format encode/decode for schema-id prefixed payloads

### Observability

- Structured logging: text, JSON, ECS (Elastic APM–friendly fields)
- In-process metrics with Prometheus HTTP handler
- Bridges for Prometheus and OpenTelemetry (no OTel SDK dependency in core)
- `log/slog` integration via `WithSlogLogger`

---

## Quick start

### Connect

```go
package main

import (
	"context"
	"log"

	"github.com/sinamohsenifar/gokafka"
)

func main() {
	cfg, err := gokafka.NewConfig(
		[]string{"localhost:9092"},
		gokafka.WithClientID("my-service"),
		gokafka.WithBrokerHostRemap(map[string]string{
			"kafka:29092": "localhost:9092", // map advertised listeners when needed
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	client, err := gokafka.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	_ = context.Background()
}
```

On connect, GoKafka negotiates API versions with the cluster. Use `client.NegotiatedAPIVersions()` or `client.ApiVersions(ctx)` to inspect supported APIs.

### Produce

```go
ctx := context.Background()
prod := client.Producer()

results, err := prod.ProduceSyncResult(ctx, gokafka.Record{
	Topic: "events",
	Key:   []byte("user-42"),
	Value: []byte(`{"type":"signup"}`),
	Headers: []gokafka.Header{
		{Key: "content-type", Value: []byte("application/json")},
	},
})
if err != nil {
	log.Fatal(err)
}
r := results[0]
log.Printf("stored at %s/%d offset %d", r.Topic, r.Partition, r.Offset)
```

Default producer settings use `acks=all` and idempotency. Override with `WithProducer`:

```go
gokafka.WithProducer(gokafka.ProducerConfig{
	Acks:        gokafka.AcksAll,
	Idempotent:  true,
	Compression: gokafka.CompressionGzip,
	BatchSize:   500,
	Linger:      10 * time.Millisecond,
}),
```

### Consume

**Handler-based (recommended for services):**

```go
cfg, _ := gokafka.NewConfig(
	[]string{"localhost:9092"},
	gokafka.WithConsumerGroup("my-service"),
	gokafka.WithAutoCommit(true), // commit after handler returns nil
)

client, _ := gokafka.NewClient(cfg)
consumer := client.Consumer([]string{"events"})

err := consumer.Run(ctx, func(ctx context.Context, rec gokafka.Record) error {
	// return non-nil to stop the runner and leave offsets uncommitted
	return process(rec)
})
```

**Manual poll loop:**

```go
consumer := client.Consumer([]string{"events"})
for {
	recs, err := consumer.Poll(ctx)
	if err != nil {
		break
	}
	for _, rec := range recs {
		if err := process(rec); err != nil {
			continue
		}
	}
	if err := consumer.Commit(ctx, recs...); err != nil {
		log.Fatal(err)
	}
}
```

### Admin

```go
admin := client.Admin()

if err := admin.CreateTopic(ctx, "events", 6, 3); err != nil {
	log.Fatal(err)
}

desc, err := admin.DescribeTopic(ctx, "events")
if err != nil {
	log.Fatal(err)
}

configs, err := admin.DescribeTopicConfigs(ctx, "events")
if err != nil {
	log.Fatal(err)
}
_ = desc
_ = configs
```

---

## Configuration reference

Options are passed to `NewConfig` as functional options:

| Option | Purpose |
|--------|---------|
| `WithClientID` | Kafka `client.id` |
| `WithConsumerGroup` | Consumer group id |
| `WithGroupInstanceID` | Static membership (`group.instance.id`) |
| `WithAutoCommit` | Auto-commit in `Consumer.Run` (default: false) |
| `WithConsumeFromBeginning` | Start at earliest offset when no commit exists |
| `WithProducer` | Acks, compression, idempotency, batching |
| `WithConsumer` | Assignor, isolation level, session timeouts |
| `WithTransaction` | Transactional id and timeout |
| `WithSecurity` | TLS and SASL settings |
| `WithBrokerHostRemap` | Remap advertised `host:port` to reachable addresses |
| `WithConnection` | Dial timeout, request timeout |
| `WithConcurrency` | Async producer and consumer worker counts |
| `WithMetrics` | Enable metrics collection |
| `WithLogFormat` / `WithLogLevel` | Structured logging |
| `WithSlogLogger` | Route logs to `log/slog` |

See [pkg.go.dev](https://pkg.go.dev/github.com/sinamohsenifar/gokafka) for full type documentation.

---

## Security

TLS and SASL are configured through `SecurityConfig` and `WithSecurity`:

```go
cfg, err := gokafka.NewConfig(
	[]string{"broker.example.com:9093"},
	gokafka.WithSecurity(gokafka.SCRAMSecurity(
		gokafka.TLSConfig{CAFile: "/etc/kafka/ca.pem"},
		"gokafka", "secret", gokafka.SASLSCRAMSHA512,
	)),
)
```

Helpers exist for common setups: `TLSOnlySecurity`, `PlainSecurity`, `SCRAMPlaintextSecurity`, `SCRAMSecurity`, and `OAuthBearerSecurity`.

For local development with the included Docker stack, see [docs/CAPABILITIES.md](docs/CAPABILITIES.md#connection--security-protocols).

---

## Transactions

Enable a transactional producer:

```go
cfg, err := gokafka.NewConfig(
	[]string{"localhost:9092"},
	gokafka.WithTransaction(gokafka.TransactionConfig{
		Enabled:         true,
		TransactionalID: "my-app-txn",
		Timeout:         2 * time.Minute,
	}),
)
```

Consume-transform-produce:

```go
txn, err := client.Producer().BeginTransaction(ctx)
if err != nil {
	log.Fatal(err)
}

gen, memberID, instID := consumer.GroupMetadata()
if err := txn.SendOffsetsToTxn(ctx, "input-group", offsets, gokafka.TxnOffsetCommitOptions{
	Generation: gen, MemberID: memberID, GroupInstanceID: instID,
}); err != nil {
	_ = txn.Abort(ctx)
	log.Fatal(err)
}
if err := txn.ProduceWithinTxn(ctx, gokafka.Record{Topic: "output", Value: result}); err != nil {
	_ = txn.Abort(ctx)
	log.Fatal(err)
}
if err := txn.Commit(ctx); err != nil {
	log.Fatal(err)
}
```

Pair consumers with `IsolationReadCommitted` when reading from transactional topics.

---

## Error handling

Broker errors are returned as `*gokafka.KafkaError` with the Kafka error code, topic, and partition when applicable:

```go
results, err := prod.ProduceSyncResult(ctx, record)
if err != nil {
	var ke *gokafka.KafkaError
	if gokafka.AsKafkaError(err, &ke) {
		log.Printf("broker error %d on %s/%d (retriable=%v)",
			ke.Code, ke.Topic, ke.Partition, ke.Retriable())
	}
}
```

Use `gokafka.IsRetriable(err)` for retry decisions. The producer retries retriable errors according to `RetryConfig`.

---

## Observability

```go
cfg, _ := gokafka.NewConfig(
	brokers,
	gokafka.WithMetrics(true, "myapp"),
	gokafka.WithLogFormat(gokafka.LogFormatJSON),
)

client, _ := gokafka.NewClient(cfg)
http.Handle("/metrics", client.PrometheusHandler())
```

Register bridges to forward metrics into existing Prometheus or OpenTelemetry pipelines without adding OTel as a direct dependency.

---

## Examples

```bash
export KAFKA_BROKERS=localhost:9092
go run ./examples/produce
go run ./examples/consume
go run ./examples/admin
```

---

## Documentation

| Document | Contents |
|----------|----------|
| [docs/CAPABILITIES.md](docs/CAPABILITIES.md) | Connection types, serialization, use-case mapping |
| [docs/KIPS.md](docs/KIPS.md) | Kafka Improvement Proposal coverage |
| [docs/KAFKA_VERSIONS.md](docs/KAFKA_VERSIONS.md) | Broker version compatibility notes |
| [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md) | Go + Kafka release matrix |
| [docs/TESTING.md](docs/TESTING.md) | Test policy and local integration setup |
| [CHANGELOG.md](CHANGELOG.md) | Release history |
| [SECURITY.md](SECURITY.md) | Security practices and reporting |

---

## Development

```bash
go test ./...
go vet ./...
```

Integration tests require a running Kafka broker:

```bash
docker compose up -d
go test -tags=integration -timeout=5m ./...
```

See [docs/CAPABILITIES.md](docs/CAPABILITIES.md#running-the-full-integration-matrix) for environment variables and the local multi-listener stack. CI runs unit and integration workflows on every push.

`go.mod` contains **no external dependencies**—only the Go standard library.

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
