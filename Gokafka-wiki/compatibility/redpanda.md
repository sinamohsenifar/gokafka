---
title: Redpanda compatibility
type: compatibility
category: Compatibility
subcategory: Redpanda
status: stable
tags: [gokafka, compatibility, redpanda]
updated: 2026-06-30
---

# Redpanda compatibility

GoKafka is **CI-verified against a real Redpanda broker** (v26.1). A dedicated `redpanda` lane in `.github/workflows/integration.yml` runs the full integration suite against `redpandadata/redpanda` every build.

## How it works
Redpanda implements the Kafka wire protocol, and GoKafka [[protocol/version-negotiation|negotiates API versions]] at connect, so it adapts automatically. Redpanda's **Schema Registry is Confluent-compatible** and works with the [[packages/schema-registry|schema]] package unchanged.

## What Redpanda doesn't implement (auto-skipped)
The test suite skips these gracefully (via `skipIfUnsupportedAPI` and capability checks):
- **ElectLeaders** (API 43) — not advertised.
- **Delegation tokens** (API 38–41) — not advertised.
- **KIP-848** next-gen consumer groups — `ConsumerGroupHeartbeat` not supported in v26.1.
- **KIP-932** share groups — not supported.
- KRaft **finalized features** (`metadata.version`) — Redpanda doesn't advertise them, so `Client.BrokerFeature` returns not-found and transactions fall back to v1 (TV1, which works).

When an admin call hits an unadvertised API, GoKafka now returns a **clear** `"broker does not support API key N (Name)"` instead of an opaque EOF (`Cluster.AdvertisesAPI` + `protocol.APIName`).

## Bugs found via Redpanda
Testing here surfaced **three real protocol bugs** (latent on Kafka) — see [[compatibility/broker-quirks]].

## Test portability changes
The suite was made broker-agnostic:
- controller id `0` is valid (was asserted invalid);
- broker ids come from `DescribeCluster` (not a hardcoded `1`);
- security tests skip when their TLS/SASL listener isn't reachable (`net.DialTimeout` probe in `integrationBrokerEnv`).

## Local repro
```bash
docker run -d --name rp -p 9092:9092 -p 8081:8081 redpandadata/redpanda:latest \
  redpanda start --mode dev-container --smp 1 --overprovisioned \
  --kafka-addr external://0.0.0.0:9092 --advertise-kafka-addr external://127.0.0.1:9092 \
  --schema-registry-addr 0.0.0.0:8081
KAFKA_BROKERS=127.0.0.1:9092 SCHEMA_REGISTRY_URL=http://127.0.0.1:8081 \
  go test -tags=integration -race -p=1 .
```

## Related
- 📄 Canonical doc: [docs/REDPANDA.md](../../docs/REDPANDA.md)
- [[compatibility/kafka-versions]] · [[protocol/version-negotiation]] · [[compatibility/broker-quirks]]
- [[protocol/api-coverage]] · [[architecture/wire-protocol]] · [[packages/schema-registry]] · [[competitors/parity-matrix]]
