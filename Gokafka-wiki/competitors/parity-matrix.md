---
title: Competitor parity matrix
type: competitor
category: Competitors
subcategory: Analysis
status: stable
tags: [gokafka, competitor, parity]
updated: 2026-06-30
---

# Competitor parity matrix

GoKafka vs the four major Go Kafka clients. Built from a competitor analysis (features, design, docs, known bugs) followed by a parity pass that closed every realistic gap.

| Capability | **GoKafka** | [[competitors/franz-go\|franz-go]] | [[competitors/kafka-go\|kafka-go]] | [[competitors/sarama\|sarama]] | [[competitors/confluent-kafka-go\|confluent]] |
|---|---|---|---|---|---|
| Dependencies | **stdlib only** | Go modules | Go modules | Go modules | CGO + librdkafka |
| Pure-Go binary | ✅ | ✅ | ✅ | ✅ | ❌ |
| Idempotent producer | ✅ | ✅ | ❌ | ✅ | ✅ |
| Transactions / EOS | ✅ (TV2) | ✅ | ❌ | ✅ | ✅ |
| KIP-848 next-gen groups | ✅ | ✅ | ❌ | ❌ | ✅ |
| KIP-932 share groups | ✅ | ✅ | ❌ | ❌ | ❌ |
| Admin client | ✅ (43 APIs) | ✅ | partial | ✅ | ✅ |
| Cross-client partitioners (murmur2 + CRC32) | ✅ | ✅ | ✅ | murmur2 | ✅ |
| Consumer-group lag | ✅ | ✅ (kadm) | manual | manual | manual |
| In-memory test mocks | ✅ broker + SR | ✅ kfake | ❌ | ✅ mocks | ✅ mock client |
| Client-side field encryption (CSFLE) | ✅ pure-Go | ❌ | ❌ | ❌ | ✅ (KMS plugins) |
| Redpanda CI-tested | ✅ | ✅ | — | — | — |

## What the parity pass added (v0.25.21 → v0.26.7)
1. CRC32 + custom partitioners ([[packages/partitioners]])
2. `Admin.ConsumerGroupLag` ([[features/consumer-lag]])
3. SR subject strategies + `MockRegistry` ([[packages/schema-registry]])
4. Examples-as-docs (transactions, share group, schema registry)
5. Clear TLS-mismatch error
6. OffsetFetch v7 `require_stable` ([[features/exactly-once-tv2]])
7. `DescribeUserScramCredentials`
8. **kfake** in-process mock broker ([[packages/kfake-mock-broker]])
9. **CSFLE** field encryption ([[features/csfle]])
10. Partition reassignments, header-based SR framing, `SchemaByGUID`
11. **Redpanda** support, CI-verified ([[compatibility/redpanda]])

## Deliberate non-goals
- **KIP-714 client metrics** — OTLP/protobuf push conflicts with stdlib-only; rich pluggable observability is provided instead ([[packages/observability]]).
- **Full in-process Kerberos/KDC** — GSSAPI is SPNEGO pass-through only.

## Related
[[competitors/franz-go]] · [[competitors/sarama]] · [[competitors/kafka-go]] · [[competitors/confluent-kafka-go]] · [[features/share-groups]] · [[features/next-gen-groups]] · [[decisions/adr-stdlib-only]] · [[protocol/api-coverage]]
