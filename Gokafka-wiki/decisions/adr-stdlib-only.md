---
title: "ADR: Stdlib-only, no CGO"
type: decision
category: Decisions
subcategory: ADR
status: accepted
tags: [gokafka, decision]
updated: 2026-06-30
---

# ADR: Stdlib-only, no CGO

## Status
Accepted (foundational constraint).

## Context
Go Kafka clients split into pure-Go (franz-go, sarama, kafka-go) and CGO wrappers (confluent-kafka-go over librdkafka). CGO buys librdkafka's maturity but costs a C toolchain, platform-specific binaries, and no single static binary.

## Decision
GoKafka uses **only the Go standard library** — no third-party `go.mod` dependencies, no CGO. Every codec (incl. snappy/lz4/zstd compression and Avro), the SASL/TLS stack, and the Schema Registry HTTP client are implemented or vendored against stdlib only.

## Consequences
- ✅ Single static binary; trivial cross-compilation; tiny dependency surface.
- ✅ Full control over the wire protocol (enabled finding the [[compatibility/broker-quirks|decode bugs]]).
- ⚠️ **KIP-714 client metrics** is a non-goal — OTLP/protobuf push needs codegen/deps. Mitigated by pluggable [[packages/observability|observability]].
- ⚠️ **CSFLE** ships the framework + a local KMS; cloud KMS drivers are left to the caller via the `KMS` interface so core stays dependency-free ([[features/csfle]]).
- ⚠️ GSSAPI is SPNEGO pass-through, not in-process Kerberos.

## Related
[[features/csfle]] · [[packages/observability]] · [[competitors/parity-matrix]] · [[competitors/confluent-kafka-go]] · [[packages/compression]] · [[architecture/overview]] · [[decisions/adr-mock-as-oracle]]
