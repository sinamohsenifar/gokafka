# Kafka Improvement Proposals (KIP) — GoKafka Support Matrix

This document maps [Apache Kafka KIPs](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals) and related protocol features to GoKafka implementation and test coverage.

**Target brokers:** Kafka 3.4+ (CI: 3.9.2–4.3.0). **Go:** 1.22+ (`go.mod`).

## Legend

| Status | Meaning |
|--------|---------|
| done | Implemented with integration or unit tests |
| partial | Wire paths exist; limited integration |
| blocked | Not planned (e.g. stdlib-only constraint) |

## Core protocol & records

| KIP | Feature | Status | Tests |
|-----|---------|--------|-------|
| [KIP-101](https://cwiki.apache.org/confluence/display/KAFKA/KIP-101+-+A+General+Purpose+Partition+Assignment+Policy) / [KIP-107](https://cwiki.apache.org/confluence/display/KAFKA/KIP-107+-+Add+deleteRecordsBefore+API) | Magic v2 record batch, `numRecords` in batch header | ✅ | `TestRecordBatchRoundTrip`, all produce/consume integration |
| KIP-31 | Timestamps in produce/fetch | ✅ | `TestIntegrationHeadersRoundTrip`, consume timestamps |
| KIP-98 | Idempotent producer | ✅ | Default producer, `TestIdempotentRequiresAcksAll` |
| KIP-130 | Idempotent producer + transactions | ✅ | `TestIntegrationTransactionEOS` |
| KIP-185 | Incremental fetch / aborted txns (fetch v4+) | ✅ | Transactional consume `read_committed` |
| KIP-219 | Request/response headers (SASL) | ✅ | Security integration suite |
| KIP-482 | Flexible versions (produce/fetch v9+/v12+) | ✅ | Flex caps raised in v0.22 |
| KIP-896 | API version baseline (4.0) | ✅ | `docs/KAFKA_VERSIONS.md`, `TestIntegrationNegotiatedVersions` |

## Compression

| Codec | Status | Tests |
|-------|--------|-------|
| none | ✅ | All integration |
| gzip | ✅ | `TestIntegrationCompressionGzip` |
| snappy | ✅ | `TestIntegrationCompressionSnappy` (literal-framed encoder) |
| lz4 | ✅ | `TestIntegrationCompressionLZ4`, `TestLZ4RoundTrip` |
| zstd | ✅ | `TestIntegrationCompressionZstd`, `TestZstdRoundTrip` |

Compression is applied only when compressed size is smaller than uncompressed (broker-safe).

## Consumer groups

| KIP | Feature | Status | Tests |
|-----|---------|--------|-------|
| KIP-62 | Consumer groups | ✅ | All group consume integration |
| KIP-394 | `MEMBER_ID_REQUIRED` join retry | ✅ | Consumer integration (implicit) |
| KIP-345 | Static membership (`group.instance.id`) | ✅ | `TestIntegrationStaticMembership` |
| KIP-429 | Cooperative sticky assignor | ✅ | `TestIntegrationCooperativeStickyRebalance` |
| KIP-848 | Consumer group protocol (next-gen) | ✅ | `TestIntegrationConsumerGroup848` |

## Admin & metadata

| Feature | Status | Tests |
|---------|--------|-------|
| Create/delete topics, partitions | ✅ | `TestIntegrationAdminTopicLifecycle` |
| Describe topic/cluster | ✅ | Admin + `TestIntegrationDescribeCluster` |
| AlterConfigs (topic) | ✅ | `TestIntegrationAlterTopicConfigs` |
| DescribeConfigs (topic/broker) | ✅ | v3 legacy wire (`VerDescribeConfigs=3`); v4 flex encoded, cap pending |
| Incremental alter configs | ✅ | v0 wire (`TestIntegrationIncrementalAlterTopicConfigs`) |
| ACL create/describe/delete | ✅ | `TestIntegrationAdminACL` |
| CreatePartitions | ✅ | v1 legacy in admin lifecycle; v2 flex encoded, cap pending |

## Security

| Feature | Status | Tests |
|---------|--------|-------|
| PLAINTEXT / SSL / mTLS | ✅ | `integration_security_test.go` |
| SASL/PLAIN, SCRAM-256/512 | ✅ | Security integration |
| SASL/GSSAPI (Kerberos) | ✅ SPNEGO | `TokenProvider` / `InitToken` |
| OAuthBearer | ✅ | `TestBuildOAuthMessage`; optional `integration && oauth` profile |

## Transactions (EOS)

| Feature | Status | Tests |
|---------|--------|-------|
| InitProducerId | ✅ | Idempotent + transactional produce |
| AddPartitionsToTxn | ✅ | `TestIntegrationTransactionEOS` |
| AddOffsetsToTxn + TxnOffsetCommit | ✅ | `TestIntegrationTransactionSendOffsets` |
| Produce within transaction | ✅ | `TestIntegrationTransactionEOS`, CTP test |
| Abort transaction | ✅ | `TestIntegrationTransactionAbort` |
| read_committed consume | ✅ | Transaction integration suite |

## Share groups

| KIP | Feature | Status | Tests |
|-----|---------|--------|-------|
| KIP-932 | Share consumer groups | ✅ | `TestIntegrationShareConsumer` (Kafka 4.1+ with `share.version=1`) |

## Best practices enforced in GoKafka

1. **Record batches** — KIP-107 `numRecords` field, per-record offset/timestamp deltas, Castagnoli CRC.
2. **API version alignment** — Request body version matches header (cluster no longer upgrades past caller version).
3. **Idempotent defaults** — `acks=all`, sequence per batch, PID epoch reset on retriable errors.
4. **Consume** — Commit-after-process default; `read_committed` for transactional topics.
5. **Compression** — Skip when expansion would occur; Kafka LZ4 framing with match-capable encoder.

## Running tests

```bash
# Unit
go test ./...

# Integration (Docker stack)
export KAFKA_BROKERS=localhost:9092
export KAFKA_BROKERS_PLAINTEXT=localhost:9092
export KAFKA_BROKERS_SSL=localhost:9093
export KAFKA_BROKERS_SASL_PLAINTEXT=localhost:9094
export KAFKA_BROKERS_SASL_SSL=localhost:9095
export GOKAFKA_SECRETS_DIR=$PWD/docker/secrets
go test -tags=integration -count=1 -timeout=5m ./...
```
