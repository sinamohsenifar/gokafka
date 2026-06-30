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

GoKafka implements **47** client-facing API keys. Versions are negotiated with
the broker at connect time, so a lower client ceiling still interoperates.

| Key | API | GoKafka max | Kafka 4.3 max | Status |
|----:|-----|:-----------:|:-------------:|:------:|
| 0 | Produce | 12 | 13 | ✅ (v12; enables KIP-890 TV2 implicit partition add; KIP-951 leader hints) |
| 1 | Fetch | 13 | 18 | ✅ (v13 topic-id fetch, KIP-516; refreshes metadata on UNKNOWN_TOPIC_ID) |
| 2 | ListOffsets | 3 | 11 | ➖ (no current-leader-epoch, v4+) |
| 3 | Metadata | 12 | 13 | ✅ |
| 8 | OffsetCommit | 8 | 10 | ➖ |
| 9 | OffsetFetch | 6 / 8 | 10 | ✅ (single-group v6; batched multi-group v8 via `Admin.FetchOffsets`, KIP-709) |
| 10 | FindCoordinator | 3 | 6 | ✅ (flexible/tagged-fields; single-key, not v4 batched) |
| 11 | JoinGroup | 6 | 9 | ➖ |
| 12 | Heartbeat | 4 | 4 | ✅ |
| 13 | LeaveGroup | 5 | 5 | ✅ |
| 14 | SyncGroup | 5 | 5 | ✅ |
| 15 | DescribeGroups | 5 | 6 | ➖ |
| 16 | ListGroups | 2 | 5 | ➖ |
| 17 | SaslHandshake | 1 | 1 | ✅ |
| 18 | ApiVersions | 3 | 4 | ✅ (flexible v3; parses cluster-finalized features) |
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
| 38 | CreateDelegationToken | 3 | 3 | ✅ |
| 39 | RenewDelegationToken | 2 | 2 | ✅ |
| 40 | ExpireDelegationToken | 2 | 2 | ✅ |
| 41 | DescribeDelegationToken | 3 | 3 | ✅ |
| 42 | DeleteGroups | 2 | 2 | ✅ |
| 43 | ElectLeaders | 2 | 2 | ✅ |
| 44 | IncrementalAlterConfigs | 0 | 1 | ➖ |
| 45 | AlterPartitionReassignments | 0 | 0 | ✅ |
| 46 | ListPartitionReassignments | 0 | 0 | ✅ |
| 47 | OffsetDelete | 0 | 0 | ✅ |
| 48 | DescribeClientQuotas | 1 | 1 | ✅ |
| 49 | AlterClientQuotas | 1 | 1 | ✅ |
| 50 | DescribeUserScramCredentials | 0 | 0 | ✅ |
| 51 | AlterUserScramCredentials | 0 | 0 | ✅ |
| 60 | DescribeCluster | ✓ | 2 | ✅ |
| 65 | DescribeTransactions | 0 | 0 | ✅ |
| 66 | ListTransactions | 2 | 2 | ✅ |
| 68 | ConsumerGroupHeartbeat (KIP-848) | 1 | 1 | ✅ |
| 69 | ConsumerGroupDescribe (KIP-848) | ✓ | 1 | ✅ |
| 76 | ShareGroupHeartbeat (KIP-932) | 1 | 1 | ✅ |
| 77 | ShareGroupDescribe (KIP-932) | 1 | 1 | ✅ |
| 78 | ShareFetch (KIP-932) | 2 | 2 | ✅ |
| 79 | ShareAcknowledge (KIP-932) | 2 | 2 | ✅ (v2 `is_renew_ack` / Renew, KIP-1222; negotiated down to v1) |

### Client-facing APIs NOT implemented

| Key | API | Relevance | Priority |
|----:|-----|-----------|----------|
| 23 | OffsetForLeaderEpoch | KIP-320 log-truncation detection (fencing already done: Fetch sends `current_leader_epoch`) | **Medium** (truncation detection on unclean failover) |
| 71/72 | GetTelemetrySubscriptions / PushTelemetry | KIP-714 client metrics push — **deliberate non-goal** (see §5) | n/a (stdlib-only constraint) |
| 75 | DescribeTopicPartitions | KIP-966 cursor-based metadata for very large clusters | Low (Metadata API 3 still works) |
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
| KIP-932 | Share groups / queues (ShareConsumer) | ✅ (Accept/Release/Reject; Renew via ShareAcknowledge v2, KIP-1222) |
| KIP-98 / KIP-447 | Idempotent producer + transactions / EOS | ✅ (transactions v1 and v2 — see KIP-890) |
| KIP-345 | Static group membership (`group.instance.id`) | ✅ |
| KIP-429 | Cooperative incremental rebalance | ✅ (cooperative-sticky) |
| KIP-896 | Drop pre-2.1 request versions; message format v0/v1 removed | ✅ (only v2 record batches; magic≠2 rejected) |
| KIP-98 read_committed | Skip aborted-transaction records | ✅ (filters by aborted-txn list) |
| SCRAM/OAUTHBEARER/GSSAPI | SASL mechanisms | ✅ (GSSAPI = SPNEGO pass-through) |
| KIP-584 | Feature versioning (cluster-finalized feature levels via ApiVersions) | ✅ (parsed and exposed via `Client.BrokerFeature`; `transaction.version` gates the KIP-890 TV2 path) |
| KIP-890 | Transactions v2 (Produce v12, implicit partition add, epoch bump) | ✅ (TV2 produce path: when `transaction.version >= 2`, skips client `AddPartitionsToTxn` — broker registers partitions implicitly on Produce v12. EndTxn v5 returns the bumped producer epoch, adopted and reused across sequential transactions (producer id constant, epoch increasing) without re-`InitProducerID`. Group-offset registration keeps `AddOffsetsToTxn`, which is not implicit. Falls back to v1 on `transaction.version < 2`.) |
| KIP-714 | Client metrics push (telemetry RPCs) | ➖ (deliberate non-goal — requires OTLP/protobuf encoding, which conflicts with the stdlib-only, zero-dependency design; rich client-side observability is provided instead, see §5) |
| KIP-899 / KIP-1102 | Rebootstrap from `bootstrap.servers` / on server signal | ➖ (refresh fails over across configured seeds; no full rebootstrap-on-signal) |
| KIP-1106 | Duration-based `auto.offset.reset` | ❌ (earliest/latest only) |
| KIP-390 | Configurable producer compression level | ➖ (`WithProducerCompressionLevel` honored for gzip; pure-Go snappy/lz4/zstd are fixed-strategy and ignore it) |
| KIP-848 RE2J | Server-side regex subscription | ✅ (`Client.ConsumerPattern(regex)`; next-gen protocol; broker resolves matching topics) |
| KIP-516 | Topic IDs (Fetch by topic-id) | ✅ (Fetch v13 sends topic UUIDs resolved from metadata; refreshes + retries on UNKNOWN_TOPIC_ID — robust to topic delete/recreate. Metadata, ShareFetch and the next-gen consumer already use topic ids.) |
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
| `POST /subjects/{subject}` (is-registered probe) | ✅ (`IsRegistered`) |
| `GET/PUT /mode[/{subject}]` (registry mode) | ✅ (`Mode` / `SetMode`; Confluent-specific — ccompat layers may return "not supported") |

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

The modern-feature surface is complete. The remaining items are one optional robustness follow-up and one deliberate, constraint-driven non-goal:

1. **OffsetForLeaderEpoch (KIP-320)** — *optional follow-up.* Leader-epoch *fencing* on Fetch is done; the remaining piece is full *truncation detection* (query OffsetForLeaderEpoch API 23 on leader change) and committed-leader-epoch on offset commit/fetch — a robustness improvement for unclean failover, not a feature gap.
2. **KIP-714 client metrics push** — *deliberate non-goal.* `GetTelemetrySubscriptions` / `PushTelemetry` require encoding metrics as **OTLP/protobuf**, which conflicts with this client's hard **stdlib-only, zero-dependency** design (no protobuf codegen). It is also the one wire format with no practical local correctness oracle (a broker accepts a telemetry push without validating its OTLP semantics), so a hand-rolled encoder could not be verified to the standard every other codec here was. Instead, GoKafka ships rich **client-side** observability — pluggable `MetricsRecorder`, Prometheus, and OpenTelemetry bridges, structured `slog`, and tracing hooks — which covers the same operational need without broker-push telemetry.

_Closed this line of work:_ KIP-890 transactions v2 (Produce v12 implicit partition-add + EndTxn v5 epoch reuse), Fetch v13 topic-ids (KIP-516), OffsetFetch v8 multi-group (KIP-709), ShareAcknowledge v2 Renew (KIP-1222), FindCoordinator flex v3, cluster feature negotiation (KIP-584), Schema Registry lifecycle (incl. `IsRegistered`, `Mode`/`SetMode`), and the consumer niceties (KIP-1106, KIP-390, KIP-848 RE2J).

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
Connection/goroutine-leak and Close-idempotency (#1/#10) are now covered by
`integration_lifecycle_test.go` (client connect/close loop and consumer
join/leave loop assert no goroutine growth). The only remaining "test
recommended" follow-up is buffered-record drop on rebalance (#3).

_Generated from a verification pass against Apache Kafka 4.3, Confluent Schema Registry docs, and a cross-library GitHub-issue audit._
