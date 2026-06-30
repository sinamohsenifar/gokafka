# GoKafka

[![CI](https://github.com/sinamohsenifar/gokafka/actions/workflows/ci.yml/badge.svg)](https://github.com/sinamohsenifar/gokafka/actions/workflows/ci.yml)
[![Integration](https://github.com/sinamohsenifar/gokafka/actions/workflows/integration.yml/badge.svg)](https://github.com/sinamohsenifar/gokafka/actions/workflows/integration.yml)
[![Compatibility](https://github.com/sinamohsenifar/gokafka/actions/workflows/compatibility.yml/badge.svg)](https://github.com/sinamohsenifar/gokafka/actions/workflows/compatibility.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/sinamohsenifar/gokafka.svg)](https://pkg.go.dev/github.com/sinamohsenifar/gokafka)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

GoKafka is a pure Go client for Apache Kafka. It speaks the Kafka binary protocol directly using only the Go standard library — **no CGO, no `librdkafka`, and no third-party modules in `go.mod`** — so `go get` is all you need and your build stays a single static binary.

You get a full-featured client in one module: idempotent and transactional producers, classic and next-generation (KIP-848) consumer groups, KIP-932 share groups, a broad admin API, TLS/SASL security, Schema Registry helpers, and pluggable metrics/tracing/logging. The API is built around `context.Context`, functional options, and typed errors, and negotiates protocol versions with your broker at connect time.

Install:

```bash
go get github.com/sinamohsenifar/gokafka@v0.25.0
```

**Requirements:** Go 1.22 or newer · Apache Kafka 3.4+ (KRaft recommended; Kafka 4.x is KRaft-only).

### Supported Apache Kafka versions

Every release is tested against the Kafka versions below (see [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md) for the full Go + broker matrix):

| Kafka version | Notes |
|---------------|-------|
| **3.9.2** | Latest 3.x (LTS bridge) |
| **4.0.2** | Supported |
| **4.1.2** | Supported |
| **4.2.1** | Supported |
| **4.3.0** | Latest |

Tested on Go 1.22 through 1.26.

---

## How it compares

Several mature Kafka clients exist for Go. They differ mainly in dependencies, deployment model, and how much of the protocol they expose.

| | **GoKafka** | [franz-go](https://github.com/twmb/franz-go) | [segmentio/kafka-go](https://github.com/segmentio/kafka-go) | [IBM/sarama](https://github.com/IBM/sarama) | [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) |
|---|-------------|----------------------------------------------|-------------------------------------------------------------|---------------------------------------------|---------------------------------------------------------------------------|
| **Dependencies** | stdlib only | Go modules | Go modules | Go modules | CGO + librdkafka |
| **Pure Go binary** | Yes | Yes | Yes | Yes | No (native libs) |
| **Protocol implementation** | In-tree | In-tree | In-tree | In-tree | librdkafka wrapper |
| **Idempotent producer** | Yes | Yes | No | Yes | Yes |
| **Transactions (EOS)** | Yes | Yes | No | Yes | Yes |
| **Consumer groups** | Yes | Yes | Yes | Yes | Yes |
| **Cooperative rebalance** | Yes | Yes | No | Yes | Yes |
| **KIP-848 next-gen groups** | Yes | Yes | No | No | Yes |
| **KIP-932 share groups** | Yes | Yes | No | No | No |
| **Admin client** | Yes | Yes | Partial | Yes | Yes |
| **ACL admin** | Yes | Yes | No | Yes | Yes |
| **zstd compression** | Yes (pure Go) | Yes | Yes | Yes | Yes |
| **GSSAPI (SPNEGO pass-through)** | Yes | Yes | No | Yes | Yes |
| **Kerberos (GSSAPI)** | SPNEGO pass-through | Yes | No | Yes | Yes |
| **Schema Registry client** | Yes (REST + wire) | Via plugins | No | No | Via schema registry client |
| **Cross-client partitioners (murmur2 + CRC32)** | Yes | Yes | Yes | murmur2 | Yes |
| **Consumer-group lag helper** | Yes | Yes (kadm) | Manual | Manual | Manual |
| **In-memory test mocks** | Broker (`kfake`) + Schema Registry | Yes (kfake) | No | Yes (mocks) | Yes (mock client) |

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
- Partitioners: **murmur2 hash** (key-based, wire-compatible with the Java client and librdkafka) and round-robin
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
- Configs: describe, alter, incremental alter (topic, broker, and **group** resources)
- Consumer groups: list, describe, delete
- Offsets: delete committed offsets for a group
- Records: **delete records** before an offset (`DeleteRecords`)
- Leadership: **elect preferred/unclean leaders** (`ElectLeaders`)
- Storage: **describe log dirs** (`DescribeLogDirs`)
- SCRAM: **create/delete user credentials** (`UpsertUserScramCredential`, KIP-554)
- Quotas: **describe/set client quotas** (`DescribeClientQuotas`, `SetClientQuota`, KIP-546)
- Transactions: **list/describe transactions** (`ListTransactions`, `DescribeTransactions`, KIP-664)
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
- `log/slog` integration: `WithSlogLogger`, `WithSlogHandler`, or route into an existing `*slog.Logger` with `WithSlogLoggerFrom`

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

// Delete records before offset 100 on partition 0.
_, _ = admin.DeleteRecords(ctx, map[string]map[int32]int64{"events": {0: 100}})

// Throttle a user to 1 MiB/s produce.
_ = admin.SetClientQuota(ctx,
	gokafka.QuotaEntity{gokafka.QuotaEntityUser: "alice"},
	gokafka.QuotaOp{Key: "producer_byte_rate", Value: 1 << 20},
)

// Manage a SCRAM credential.
_ = admin.UpsertUserScramCredential(ctx, "alice", gokafka.ScramSHA256, "s3cret", 4096)

// Inspect in-flight transactions.
txns, _ := admin.ListTransactions(ctx, nil, nil)
_ = txns
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

Route logs into your application's existing `log/slog` setup (its handler, attributes, and level):

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "myapp")
cfg, _ := gokafka.NewConfig(brokers, gokafka.WithSlogLoggerFrom(logger))
```

Register bridges to forward metrics into existing Prometheus or OpenTelemetry pipelines without adding OTel as a direct dependency. Custom `Logger`, `Tracer`, and `MetricsRecorder` implementations can be supplied with `WithLogger`, `WithTracer`, and `WithMetricsHook`.

---

## Examples

Each directory under [`examples/`](examples) is a self-contained runnable program.

```bash
export KAFKA_BROKERS=localhost:9092
go run ./examples/produce        # produce a record
go run ./examples/consume        # consumer group
go run ./examples/admin          # topic / cluster admin
go run ./examples/transactions   # exactly-once (EOS) produce + commit
go run ./examples/sharegroup     # KIP-932 share group (queue semantics)
go run ./examples/schemaregistry # Avro serde — runs with no broker (in-memory registry)
```

### Testing without a broker

The [`kfake`](kfake) package is an in-process mock broker, so you can unit-test
producer / consumer / admin code against the real client with no Docker or
cluster:

```go
b, _ := kfake.NewBroker()
defer b.Close()
b.AddTopic("events", 1)

cfg, _ := gokafka.NewConfig([]string{b.Addr()})
client, _ := gokafka.NewClient(cfg)
// produce, consume (groups), commit, admin, and lag all work against b
```

For Schema Registry serde tests, `schema.MockRegistry` is an in-memory registry
with the identical `Serde` API.

---

## Documentation

| Document | Contents |
|----------|----------|
| [docs/CONFORMANCE.md](docs/CONFORMANCE.md) | Protocol API / KIP / Schema Registry coverage vs Apache Kafka 4.3 |
| [docs/PERFORMANCE.md](docs/PERFORMANCE.md) | Tuning, benchmarks, best practices, and anti-patterns |
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
