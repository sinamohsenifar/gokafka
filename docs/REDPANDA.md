# Redpanda

GoKafka is **CI-verified against a real [Redpanda](https://redpanda.com) broker** every
build. Redpanda implements the Kafka wire protocol, and GoKafka
[negotiates API versions](ARCHITECTURE.md#version-negotiation) at connect, so it adapts
automatically.

## Status

A dedicated `redpanda` job in
[`.github/workflows/integration.yml`](../.github/workflows/integration.yml) runs the
**full integration suite** against `redpandadata/redpanda` on every push/PR. The suite
passes, skipping only the APIs Redpanda does not implement.

Redpanda's built-in **Schema Registry is Confluent-compatible** and works with the
[`schema`](../schema) package unchanged.

## What works

Everything in the core surface: produce/consume, idempotent + transactional produce
(EOS, transactions v1), `read_committed`, consumer groups (classic, static membership,
cooperative-sticky), consumer-group lag, all four compression codecs, admin
(topics/configs/ACLs/quotas/SCRAM/reassignments), and Schema Registry serdes.

## What Redpanda doesn't implement (auto-skipped)

The test suite skips these gracefully (capability checks / `skipIfUnsupportedAPI`):

| Feature | Reason |
|---|---|
| `ElectLeaders` (API 43) | not advertised |
| Delegation tokens (API 38–41) | not advertised |
| KIP-848 next-gen consumer groups | `ConsumerGroupHeartbeat` unsupported in the tested version |
| KIP-932 share groups | unsupported |
| KRaft finalized features (`metadata.version`) | not advertised; `Client.BrokerFeature` returns not-found and transactions use v1 (TV1) |

When an admin call hits an unadvertised API, GoKafka returns a clear
`"broker does not support API key N (Name)"` error instead of an opaque connection EOF.

## Bugs found via Redpanda

Testing against a second Kafka-compatible implementation surfaced **three real protocol
decode bugs** that were latent against Apache Kafka (see the
[decode-bug pattern](ARCHITECTURE.md#the-decode-bug-pattern-)):

1. **DescribeConfigs v4** — missing the per-*synonym* tag section. Latent because a
   fresh Kafka topic has no synonyms; Redpanda returns them for overridden configs.
   (v0.26.6)
2. **Version negotiation** — dropped APIs a broker advertises at max version 0 (e.g.
   `ListTransactions`), so the client sent a higher version and the broker reset the
   connection. (v0.26.6)
3. **CreatePartitions v2** — missing the per-topic `error_message`. Latent because Kafka
   returns null there; Redpanda returns a non-null message. (v0.26.7)

All three also harden GoKafka against **Kafka** (any non-default config or non-null
broker message would have tripped them).

## Run it locally

```bash
docker run -d --name rp -p 9092:9092 -p 8081:8081 redpandadata/redpanda:latest \
  redpanda start --mode dev-container --smp 1 --overprovisioned \
  --kafka-addr external://0.0.0.0:9092 \
  --advertise-kafka-addr external://127.0.0.1:9092 \
  --schema-registry-addr 0.0.0.0:8081

KAFKA_BROKERS=127.0.0.1:9092 SCHEMA_REGISTRY_URL=http://127.0.0.1:8081 \
  go test -tags=integration -race -p=1 .
```

The integration suite is **broker-agnostic**: it derives broker ids from
`DescribeCluster` (not a hardcoded id), accepts controller id `0`, and skips TLS/SASL
tests when those listeners aren't reachable — so the same suite runs on Kafka and
Redpanda.

## Other compatible backends

GoKafka is expected to work against other Kafka-API backends (Confluent Platform/Cloud,
Amazon MSK, Azure Event Hubs' Kafka endpoint) via the same version negotiation, but only
Apache Kafka and Redpanda are part of CI.

See also: [CONFORMANCE.md](CONFORMANCE.md) · [KAFKA_VERSIONS.md](KAFKA_VERSIONS.md) ·
[COMPATIBILITY.md](COMPATIBILITY.md).
