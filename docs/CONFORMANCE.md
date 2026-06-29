# Protocol & Ecosystem Conformance

This document records GoKafka's coverage of the Apache Kafka wire protocol, the
client-relevant KIPs across releases 3.4–4.3, and the Confluent Schema Registry
REST API / wire format. It is verified against the authoritative sources:

- Apache Kafka 4.3 message definitions (`clients/.../common/message/*.json`) and `ApiKeys.java`
- Apache Kafka protocol guide — https://kafka.apache.org/protocol
- Kafka release announcements 3.4 → 4.3 and the KIP wiki
- Confluent Schema Registry API & SerDes docs — https://docs.confluent.io/platform/current/schema-registry/

Legend: ✅ implemented · ➖ implemented below the broker's max version (works via
version negotiation; newer revision unused) · ❌ not implemented · n/a broker/controller-internal.

---

## 1. Protocol API coverage

GoKafka implements **40** client-facing API keys. Versions are negotiated with
the broker at connect time, so a lower client ceiling still interoperates.

| Key | API | GoKafka max | Kafka 4.3 max | Status |
|----:|-----|:-----------:|:-------------:|:------:|
| 0 | Produce | 9 | 13 | ➖ (transactions v1; not KIP-890 txn v2) |
| 1 | Fetch | 12 | 18 | ➖ (topic-name fetch; not topic-id fetch v13+) |
| 2 | ListOffsets | 3 | 11 | ➖ (no current-leader-epoch, v4+) |
| 3 | Metadata | 12 | 13 | ✅ |
| 8 | OffsetCommit | 8 | 10 | ➖ |
| 9 | OffsetFetch | 5 | 10 | ➖ (no batched multi-group, v8+) |
| 10 | FindCoordinator | 1 | 6 | ➖ (legacy v1; no batched keys) |
| 11 | JoinGroup | 6 | 9 | ➖ |
| 12 | Heartbeat | 4 | 4 | ✅ |
| 13 | LeaveGroup | 5 | 5 | ✅ |
| 14 | SyncGroup | 5 | 5 | ✅ |
| 15 | DescribeGroups | 5 | 6 | ➖ |
| 16 | ListGroups | 2 | 5 | ➖ |
| 17 | SaslHandshake | 1 | 1 | ✅ |
| 18 | ApiVersions | 2 | 4 | ➖ |
| 19 | CreateTopics | 4 | 7 | ➖ |
| 20 | DeleteTopics | 6 | 6 | ✅ |
| 21 | DeleteRecords | 2 | 2 | ✅ |
| 22 | InitProducerId | ✓ | 6 | ✅ |
| 24 | AddPartitionsToTxn | ✓ | 5 | ✅ |
| 25 | AddOffsetsToTxn | ✓ | 4 | ✅ |
| 26 | EndTxn | ✓ | 5 | ✅ |
| 28 | TxnOffsetCommit | ✓ | 5 | ✅ |
| 29 | DescribeAcls | ✓ | 3 | ✅ |
| 30 | CreateAcls | ✓ | 3 | ✅ |
| 31 | DeleteAcls | ✓ | 3 | ✅ |
| 32 | DescribeConfigs | 4 | 4 | ✅ |
| 33 | AlterConfigs | 2 | 2 | ✅ (legacy; prefer IncrementalAlterConfigs) |
| 35 | DescribeLogDirs | 5 | 5 | ✅ |
| 36 | SaslAuthenticate | 1 | 2 | ➖ |
| 37 | CreatePartitions | 2 | 3 | ➖ |
| 42 | DeleteGroups | 2 | 2 | ✅ |
| 43 | ElectLeaders | 2 | 2 | ✅ |
| 44 | IncrementalAlterConfigs | 0 | 1 | ➖ |
| 47 | OffsetDelete | 0 | 0 | ✅ |
| 48 | DescribeClientQuotas | 1 | 1 | ✅ |
| 49 | AlterClientQuotas | 1 | 1 | ✅ |
| 51 | AlterUserScramCredentials | 0 | 0 | ✅ |
| 60 | DescribeCluster | ✓ | 2 | ✅ |
| 65 | DescribeTransactions | 0 | 0 | ✅ |
| 66 | ListTransactions | 2 | 2 | ✅ |
| 68 | ConsumerGroupHeartbeat (KIP-848) | 1 | 1 | ✅ |
| 69 | ConsumerGroupDescribe (KIP-848) | ✓ | 1 | ✅ |
| 76 | ShareGroupHeartbeat (KIP-932) | 1 | 1 | ✅ |
| 77 | ShareGroupDescribe (KIP-932) | 1 | 1 | ✅ |
| 78 | ShareFetch (KIP-932) | 2 | 2 | ✅ |
| 79 | ShareAcknowledge (KIP-932) | 1 | 2 | ➖ (v1; 4.2 added v2 `RENEW`) |

### Client-facing APIs NOT implemented

| Key | API | Relevance | Priority |
|----:|-----|-----------|----------|
| 23 | OffsetForLeaderEpoch | KIP-320 log-truncation detection (fencing already done: Fetch sends `current_leader_epoch`) | **Medium** (truncation detection on unclean failover) |
| 71/72 | GetTelemetrySubscriptions / PushTelemetry | KIP-714 client metrics push (broker only advertises if a telemetry reporter is configured) | **Medium** (observability) |
| 50 | DescribeUserScramCredentials | Read SCRAM creds (we implement Alter=51, not Describe) | Low (admin completeness) |
| 75 | DescribeTopicPartitions | KIP-966 cursor-based metadata for very large clusters | Low (Metadata API 3 still works) |
| 45/46 | Alter/ListPartitionReassignments | Partition reassignment admin | Low |
| 38–41 | Delegation tokens | Create/Renew/Expire/Describe delegation token auth | Low (niche) |
| 34 | AlterReplicaLogDirs | Move replicas between log dirs | Low |
| 55, 57, 61, 64, 80, 81 | DescribeQuorum, UpdateFeatures, DescribeProducers, UnregisterBroker, Add/RemoveRaftVoter | KRaft/feature/admin operations | Low (operational tooling) |
| 90–92 | Describe/Alter/DeleteShareGroupOffsets | KIP-932 share-group offset admin | Low |

Not applicable (broker/controller-internal, or non-client): 4–7 (removed in 4.0),
27 WriteTxnMarkers, 52–54/56/58–59/62–63/67/70/73/82–87 (KRaft & coordinator
internals), 88/89 Streams groups (KIP-1071).

---

## 2. KIP / release-feature coverage (3.4 → 4.3)

| KIP | Feature | Status |
|-----|---------|:------:|
| KIP-848 | Next-gen consumer group protocol (`group.protocol=consumer`) | ✅ |
| KIP-932 | Share groups / queues (ShareConsumer) | ✅ (v1; `RENEW` ack from ShareAcknowledge v2 ❌) |
| KIP-98 / KIP-447 | Idempotent producer + transactions / EOS | ✅ (transaction protocol v1) |
| KIP-345 | Static group membership (`group.instance.id`) | ✅ |
| KIP-429 | Cooperative incremental rebalance | ✅ (cooperative-sticky) |
| KIP-896 | Drop pre-2.1 request versions; message format v0/v1 removed | ✅ (only v2 record batches; magic≠2 rejected) |
| KIP-98 read_committed | Skip aborted-transaction records | ✅ (filters by aborted-txn list) |
| SCRAM/OAUTHBEARER/GSSAPI | SASL mechanisms | ✅ (GSSAPI = SPNEGO pass-through) |
| KIP-890 | Transactions v2 (Produce v10+, server-side verify) | ❌ (uses txn v1; interoperates with 4.x brokers) |
| KIP-714 | Client metrics push (telemetry RPCs) | ❌ |
| KIP-899 / KIP-1102 | Rebootstrap from `bootstrap.servers` / on server signal | ➖ (refresh fails over across configured seeds; no full rebootstrap-on-signal) |
| KIP-1106 | Duration-based `auto.offset.reset` | ❌ (earliest/latest only) |
| KIP-390 | Configurable producer compression level | ➖ (`WithProducerCompressionLevel` honored for gzip; pure-Go snappy/lz4/zstd are fixed-strategy and ignore it) |
| KIP-848 RE2J | Server-side regex subscription | ✅ (`Client.ConsumerPattern(regex)`; next-gen protocol; broker resolves matching topics) |
| KIP-320 | Leader-epoch fencing | ➖ (Fetch sends `current_leader_epoch` from metadata and refreshes+retries on NOT_LEADER/FENCED/UNKNOWN_LEADER_EPOCH; full truncation detection via OffsetForLeaderEpoch API 23 is a follow-up) |
| KIP-1139 / KIP-1258 | OAuth `jwt-bearer` grant / client-assertion | ➖ (token provider is pluggable; specific grants are caller-supplied) |

Server-side / interop-only (a client does not implement): KIP-405 tiered storage,
KRaft internals, ZK→KRaft migration, Streams/Connect/MirrorMaker KIPs.

---

## 3. Confluent Schema Registry conformance

### Wire format
- **Avro / JSON Schema**: `0x00` magic + 4-byte big-endian schema id + payload. ✅
- **Protobuf**: magic + id + message-index section (zigzag-varint count + indexes;
  `[0]` collapses to a single `0x00`) + payload. ✅ (count zigzag encoding fixed to
  match `KafkaProtobufSerializer`).

### Serialization support
| Type | Support |
|------|---------|
| Avro | ✅ full encode/decode (Avro binary) |
| JSON Schema | ✅ full encode/decode |
| Protobuf | ✅ Confluent wire framing only — you provide the encoded protobuf bytes (no protobuf codegen, to stay stdlib-only) |

Schema-ID pinning / allow-list on decode is supported (`ExpectedSchemaID`,
`PinRegisteredSchemaID`, `AllowedSchemaIDs`).

### REST endpoints
| Endpoint | Status |
|----------|:------:|
| `POST /subjects/{subject}/versions` (register) | ✅ |
| `GET /schemas/ids/{id}` (fetch by id) | ✅ |
| Content-Type `application/vnd.schemaregistry.v1+json` | ✅ |
| Apicurio ccompat base path (`/apis/ccompat/v6\|v7`) | ✅ (configurable URL) |
| `POST /compatibility/subjects/...` (compatibility check) | ✅ (`IsCompatible`) |
| `GET/PUT /config[/{subject}]` (compatibility level) | ✅ (`Compatibility` / `SetCompatibility`) |
| `GET /subjects`, `GET /subjects/{s}/versions[/{v}]` | ✅ (`ListSubjects` / `ListVersions` / `SchemaByVersion`) |
| soft/hard delete | ✅ (`DeleteSubject` / `DeleteSubjectVersion`, `permanent` flag) |
| `TopicNameStrategy` subject naming helper | ✅ (`SubjectForTopic`) |
| `POST /subjects/{subject}` (is-registered) / `/mode` | ❌ (minor) |

The Schema Registry client covers the produce/consume serde path **and** schema
lifecycle management (compatibility checks, config, version listing, deletes).

---

## 4. Producer / consumer / security / compression

- **Producer**: idempotent, transactional (EOS, consume-transform-produce), sync /
  async / batch, sticky-on-recovery retries; partitioners: **murmur2** (Kafka-compatible),
  round-robin (atomic). Compression: none, gzip, snappy, lz4, **zstd** (pure-Go).
- **Consumer**: classic groups + KIP-848 next-gen; assignors range / round-robin /
  sticky / cooperative-sticky; static membership; pause/resume; seek; **read_committed**
  (filters aborted records); rebalance listeners; KIP-932 share consumer.
- **Admin**: topics, partitions, configs (alter/incremental/describe), ACLs, consumer
  groups (list/describe/delete/offset-delete), DescribeCluster, DescribeLogDirs,
  DeleteRecords, ElectLeaders, client quotas, AlterUserScramCredentials, list/describe
  transactions.
- **Security**: TLS, mTLS, SASL PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512 / OAUTHBEARER
  (with refresh) / GSSAPI (SPNEGO pass-through). Resource limits guard against
  oversized/compression-bomb payloads.
- **Robustness**: API-version negotiation; metadata refresh with seed failover;
  retriable transport/coordinator/leader errors; verified against a 3-broker
  cluster with leader failover.

---

## 5. Prioritized gaps (roadmap)

1. **OffsetForLeaderEpoch (KIP-320)** — leader-epoch *fencing* on Fetch is done; remaining: full *truncation detection* (query OffsetForLeaderEpoch API 23 on leader change) and committed-leader-epoch on offset commit/fetch.
2. **KIP-890 transactions v2** — adopt Produce v10+ and the newer Add/EndTxn flow.
3. **KIP-714 client metrics** — `GetTelemetrySubscriptions` / `PushTelemetry`.
4. **Newer API revisions** — Fetch v13+ (topic IDs), FindCoordinator v3+/batched, ShareAcknowledge v2 (`RENEW`), OffsetFetch v8+.
5. _(Consumer niceties closed: KIP-1106 `WithConsumeSince`, KIP-390 `WithProducerCompressionLevel`, KIP-848 RE2J `ConsumerPattern`.)_
6. **Schema Registry** — remaining minor endpoints (`/mode`, is-registered probe).

---

## 6. Hardening vs known client bug classes (cross-library audit)

Synthesized from the GitHub issue trackers and changelogs of franz-go, IBM/sarama,
segmentio/kafka-go, and confluent-kafka-go/librdkafka — the recurring client
correctness bugs and how GoKafka guards against each.

| # | Bug class (seen across libraries) | GoKafka guard | Status |
|---|-----------------------------------|---------------|:------:|
| 1 | Deadlock/hang on `Close` | `Close` closes pooled conns; consumer/share heartbeat goroutines cancelled via context; async producer `Close` is `sync.Once`-idempotent | ✅ |
| 2 | Offset-commit / rebalance generation race | Rejoin on `REBALANCE_IN_PROGRESS` / illegal generation; commit backoff is context-aware; partial commit never advances uncommitted partitions | ✅ |
| 3 | Stale-message replay after rebalance | Absolute record offsets; control-marker offset advance; cooperative incremental revoke/assign | ✅ (buffered-drop test recommended) |
| 4 | Idempotent producer `OUT_OF_ORDER_SEQUENCE` / fatal under churn | Reset producer id on `INVALID_PRODUCER_EPOCH` / `OUT_OF_ORDER_SEQUENCE`; per-partition sequence map mutex-guarded; rollback on partial failure | ✅ |
| 5 | Producer hang on `NOT_LEADER` + metadata churn | Produce retries refresh metadata; transport errors retriable; patient bounded retry | ✅ |
| 6 | Transaction coordinator fencing / hanging txn | Patient coordinator retry; `CONCURRENT_TRANSACTIONS` retriable; all coordinator ops context-bounded | ✅ (transactions v1) |
| 7 | Metadata refresh storms / `NOT_LEADER` loops | TTL-gated refresh, topic-scoped, capped backoff, seed failover | ✅ |
| 8 | Leader-epoch / log-truncation | Fetch sends `current_leader_epoch`; refresh+retry on FENCED/UNKNOWN/NOT_LEADER; transport EOF treated as retriable (not truncation) | ➖ (fencing ✅; full truncation detection via OffsetForLeaderEpoch is a follow-up) |
| 9 | Fetch decompression edge cases (lz4/zstd) | Decompression errors surfaced (never swallowed); decompressed-size cap; reject non-v2 (magic≠2) batches | ✅ |
| 10 | Connection / goroutine leak on failover | Dead seed connection dropped+closed; `Invalidate` closes per-broker conns; `Close` joins goroutines | ✅ (leak-loop test recommended) |
| 11 | Record loss on auto-commit / unclean shutdown | Commit advances only acked records; partial commit doesn't advance uncommitted | ✅ |
| 12 | Large-message handling | Per-partition broker error code surfaced as typed `*KafkaError` (e.g. `MESSAGE_TOO_LARGE`) | ✅ |

GoKafka has regression tests for these: read_committed aborted filtering,
leader-epoch failover, seed failover, coordinator startup retries, and a
dedicated retry/error-classification suite (`errors_test.go`) that locks in
retriable-error and transport-failure handling (covers #2/#4/#5/#6/#7/#8/#10).
The only remaining "test recommended" follow-ups are integration tests for
buffered-record drop on rebalance (#3) and a connection-leak failover loop (#10).

_Generated from a verification pass against Apache Kafka 4.3, Confluent Schema Registry docs, and a cross-library GitHub-issue audit._
