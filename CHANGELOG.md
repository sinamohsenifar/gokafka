# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.21.0] - 2026-06-24

### Added

- **ZSTD compression** — pure-Go encoder/decoder for Kafka codec 4 (`internal/compress/zstd/`); produce and fetch supported with `CompressionZstd`
- Integration test `TestIntegrationCompressionZstd`
- **Fetch buffer pools** — `internal/bufpool` reused for broker response reads (`internal/transport/conn.go`)
- **GSSAPI SPNEGO pass-through** — multi-round SASL via `KerberosConfig.InitToken` and `KerberosConfig.TokenProvider`
- **KIP-848 (experimental)** — `ConsumerGroupHeartbeat` wire + `GroupProtocolNextGen` consumer path; metadata topic IDs (v10+); integration test `TestIntegrationConsumerGroup848`

### Changed

- `CompressionZstd` is no longer rejected at config validation
- Metadata negotiation cap raised to v12 for topic UUID resolution (KIP-848 assignments)

## [0.20.14] - 2026-06-23

### Fixed

- **AddOffsetsToTxn** — correct Kafka 4 wire format (registers `group_id` only; topic/partition offsets belong in TxnOffsetCommit)
- **TxnOffsetCommit** — encode `committed_leader_epoch` (v2+) and group metadata (v3+); flex response decode with legacy fallback
- **SendOffsetsToTxn** — `TxnOffsetCommitOptions` + `Consumer.GroupMetadata()` for consume-transform-produce EOS
- **INVALID_TXN_STATE (48)** — error code constant aligned with Kafka 4.x

### Added

- Full **CTP integration test** — `SendOffsetsToTxn`, transactional produce, offset advance, and `read_committed` verification

## [0.20.13] - 2026-06-23

### Security

- **Broker allowlist** — `ConnectionConfig.AllowedBrokerHosts` rejects metadata-advertised broker hostnames before dial (SSRF hardening)
- **Schema pinning** — `SerdeConfig.ExpectedSchemaID`, `PinRegisteredSchemaID`, and `AllowedSchemaIDs` validate wire schema IDs on Avro decode

### Added

- **Sticky assignor** — balanced sticky partition assignment for `sticky` and `cooperative-sticky` protocols
- **Cooperative incremental rebalance** — cooperative assignors revoke/assign only changed partitions during rebalance
- **Integration tests** — transactional abort and consume-transform-produce (`SendOffsetsToTxn`) coverage

### Changed

- **Parallel broker I/O** — consumer fetch and producer send fan out per broker concurrently
- **Async producer** — workers micro-batch records using producer `BatchSize` and `Linger`
- **Metrics** — reuse static produce/consume label maps; skip hook dispatch when no hooks registered

## [0.20.12] - 2026-06-23

### Security

- **Resource limits** — cap Kafka response frames, decompressed batch size, SCRAM PBKDF2 iterations, and Schema Registry HTTP bodies
- **Schema Registry** — URL-escape subject paths; truncate error response bodies in errors
- **DeleteACLs** — reject empty name+principal filters (use `"*"` explicitly)

### Fixed

- **Multi-member consumer groups** — group leader runs range/roundrobin assignor before SyncGroup
- **Offset commit** — partial `Commit(records...)` no longer advances uncommitted partitions; decode commit/heartbeat/sync errors
- **Idempotent produce** — roll back all partition sequences on any multi-broker partial failure
- **Offset commit responses** — version-aware encode/decode with flex fallback when brokers return compact responses on legacy request versions
- **Consumer reliability** — cache coordinator; background heartbeat after join; mutex-protected consumer state; partial commit no longer advances uncommitted partitions
- **Metadata** — TTL-based refresh (`ConnectionConfig.MetadataTTL`) instead of every produce call
- **Performance** — reuse CRC32C table; inline FNV-1a partitioner (no per-record hasher alloc)

## [0.20.11] - 2026-06-23

### Fixed

- **Coordinator NOT_COORDINATOR (16)** — retry JoinGroup, InitProducerId, and FindCoordinator when the broker reports a stale coordinator
- **Transactional produce** — refresh metadata and retry on retriable broker errors (same as `ProduceSync`)
- **Integration topic readiness** — poll partition metadata after `CreateTopic` instead of a fixed sleep (compression, headers, batch, and related tests)

## [0.20.10] - 2026-06-23

### Fixed

- **Integration test stability** — poll for partition metadata after topic admin ops; run CI integration tests with `-p=1` to reduce flakes under `-race`

## [0.20.9] - 2026-06-23

### Fixed

- **CI integration tests** — use `127.0.0.1` for broker/schema-registry endpoints so Linux runners do not dial `localhost` as IPv6 (`::1`) while Docker publishes IPv4 only

## [0.20.8] - 2026-06-23

### Fixed

- **CI TLS permissions** — world-readable keystore/credential files so the Kafka container user can read mounted secrets on Linux runners

## [0.20.7] - 2026-06-23

### Fixed

- **CI Kafka wait** — tolerate empty `docker compose ps` output under `pipefail` while the container is still being created
- **TLS cred files** — write keystore passwords without a trailing newline (apache/kafka docker convention)

## [0.20.6] - 2026-06-23

### Fixed

- **CI Kafka wait** — do not treat a still-starting container as a crash during the readiness loop

## [0.20.5] - 2026-06-23

### Fixed

- **CI TLS** — stop tracking generated keystores/credentials in git; install Java in CI and verify keystore after `gen-test-certs.sh` so Kafka does not exit on SSL credential mismatch

## [0.20.4] - 2026-06-23

### Fixed

- **FindCoordinator** — retry on coordinator loading / not-available (errors 14 and 15); fixes transactional EOS integration on fresh brokers
- **CI integration stack** — use `docker compose --wait`, longer Kafka healthcheck, bounded heap for GitHub Actions runners; stop committing generated keystores (always regenerate in CI)

## [0.20.3] - 2026-06-23

### Fixed

- **Flex request header** — ClientId remains a legacy STRING on header v2; only tagged fields are flex (fixes DescribeConfigs v4, CreatePartitions v2 EOF)
- **OffsetCommit v7** — `groupInstanceId`, leader epoch; no retention field after v5
- **OffsetDelete** — route to broker; fix v0 response decode field order
- **DescribeConfigs** client cap raised to v4
- **CreatePartitions** client cap raised to v2

### Changed

- Trimmed package docs and re-export boilerplate; aligned version claims with CI (3.9.2–4.3.0)
- **OffsetDelete** integration test for `DeleteConsumerGroupOffsets`

## [0.20.2] - 2026-06-23

### Fixed

- **DescribeACLs response decode** — top-level error code + resources (was mis-parsing per-resource errors)
- **OffsetDelete v0** — legacy STRING wire instead of compact flex encoding
- **IncrementalAlterConfigs response** — version-aware flex/legacy decoder
- **AlterConfigs flex v2** — `config_source` as INT8 (future cap bump)
- **DescribeACLs filter** — null resource name when filtering broadly
- **Compatibility CI** — schema registry wait + expanded smoke test set

## [0.20.1] - 2026-06-23

### Fixed

- **IncrementalAlterConfigs** — correct `Name` / `ConfigOperation` / `Value` field order (v0 wire); integration test passes
- **ACL API keys** — `CreateAcls`=30, `DescribeAcls`=29 (was swapped)
- **Git remote** — document SSH clone `git@github.com:sinamohsenifar/gokafka.git`

## [0.20.0] - 2026-06-23

### Added

- **Multi-Kafka compatibility CI** — `.github/workflows/compatibility.yml` matrix (3.9.2–4.3.0)
- **Parameterized docker-compose** — `KAFKA_IMAGE` env (default `apache/kafka:3.9.2`)
- **Schema Serde** — `schema.Serde` / `NewSchemaSerde` with Avro binary, JSON, Protobuf wire framing
- **Registry REST** — `RegisterAvro`, `RegisterProtobuf`, `RegisterJSONSchema`
- **Docs** — `docs/TESTING.md`, `docs/COMPATIBILITY.md`, `docs/ZSTD.md`, `docs/GSSAPI.md`, `docs/LABELS.md`
- **GitHub** — issue templates, PR template, compatibility workflow badge path
- **ACL wire** — fixed swapped CreateAcls/DescribeAcls API keys; CreateACLs integration passes
- **ZSTD** — frame detection helper + documented roadmap (`internal/compress/zstd.go`)

### Changed

- **README** — Apache supported releases table, `go get @v0.20.0`
- **SECURITY.md** — supported 0.18.x / 0.19.x / 0.20.x
- **CONTRIBUTING.md** — integration tests required on protocol changes

## [0.19.0] - 2026-06-23

### Fixed

- **`Consumer.Rebalance`** — set `group` from config before join (was empty → broker `INVALID_REQUEST` / error 24)
- **`WithConsumer` / `WithProducer`** — merge partial config instead of replacing entire struct (fixes `WithConsumeFromBeginning` wiped when using `WithConsumer`)
- **Transactional produce** — include `transactional_id` in Produce request body (v3+); fixes error 53 (`TRANSACTIONAL_ID_AUTHORIZATION_FAILED`)
- **EndTxn** — legacy v2 decode path (no flex tag skip on Kafka 3.9)
- **AlterConfigs** — legacy v1 wire + correct response field order; cap `VerAlterConfigs=1`
- **Fetch decode** — multi-batch records, skip control records, safe null key lengths; fixes `read_committed` transactional consume
- **`SeekToBeginning` / `SeekToEnd`** — pass consumer isolation level to ListOffsets
- **`WriteCompactNullableString(null)`** — KIP-482 null prefix `0` (was `1`)

### Added

- **KIP integration tests** — static membership (KIP-345), cooperative-sticky join (KIP-429), AlterTopicConfigs
- **`integrationWaitTopicReady`** helper after admin topic create
- Unit tests: compact nullable string, consumer rebalance group guard, produce transactional_id encoding

## [0.18.0] - 2026-06-23

### Fixed

- **DescribeConfigs** — flex wire only at API v4+ (v1–v3 use legacy header/body); cap client at v3 for Kafka 3.9; fix v1+ response decode (`config_source` replaces `is_default`)
- **CreatePartitions** — legacy null replica assignment (`-1` not `0`); versioned response decode (legacy v0–v1 vs flex v2+); cap client at v1 until flex request validated
- **Cluster responses** — strip flexible response header tag sections via `ResponseBodyForAPI` in `Cluster.Request` / `RequestViaSeed`
- **Producer batches** — use producer id/epoch/sequence `-1` for non-idempotent produce (Kafka convention)
- **Integration admin test** — `DescribeTopicConfigs` and `CreatePartitions` assertions restored (no skip)

### Changed

- **docker-compose** — `transactional.id.authorization.enable=false` for local EOS testing alongside StandardAuthorizer

## [0.17.0] - 2026-06-23

### Added

- **`docs/KIPS.md`** — KIP / feature support matrix with test coverage notes
- **Integration tests** — gzip/snappy/lz4 compression, admin lifecycle, transactional EOS (skips on txn-id ACL), expanded security profiles
- **LZ4 producer** — Kafka-framed LZ4 with match-capable block encoder
- **DescribeConfigs** — controller routing; flex v2+ request path; legacy v1 response decode fixes

### Fixed

- **Record batch (KIP-107)** — `numRecords` in batch header (was incorrectly prefixed on records payload); per-record offset/timestamp deltas; fixes `INVALID_RECORD` (87) on Kafka 3.9
- **Cluster request versioning** — do not upgrade API version above caller-encoded body version (fixes DescribeConfigs / txn wire mismatches)
- **AddPartitionsToTxn** — legacy v1 string wire for Kafka 3.9 compatibility
- **ACL operation codes** — align with Kafka enum (READ=3, WRITE=4, …)
- **Snappy** — reliable literal-framed encoder; decode offset fix for copy mode 1
- **Compression** — only set batch compression attribute when payload shrinks
- **Removed** `debug_join_test.go` from integration suite

### Changed

- Integration env: `KAFKA_BROKERS_PLAINTEXT` should be `localhost:9092` (9094 is SASL_PLAINTEXT)

## [0.16.0] - 2026-06-23

### Added

- **Kafka 3.4+ compatibility guide** — `docs/KAFKA_VERSIONS.md`
- **`TopicSpec` / `CreateTopics`** — create topics with configs (`cleanup.policy`, retention, etc.)
- **`DescribeBrokerConfigs`** — broker-level config admin
- **Record header helpers** — `SetHeader`, `GetHeader`, `WithHeaders`, `HeaderRecord`
- **GSSAPI/Kerberos config types** — `KerberosConfig`, `SASLGSSAPI` (implementation pending)
- **Integration tests** — admin topic lifecycle, headers round-trip, batch produce (10 records/request), ACL (skips if authorizer off)
- **Docker StandardAuthorizer** — optional ACL testing (`KAFKA_AUTHORIZER_CLASS_NAME`)

### Fixed

- **Produce throughput** — multiple records per partition batched into one `RecordBatch` (major performance win)
- **Idempotent sequences** — one sequence per batch, correct `lastOffsetDelta`
- **Broker connection race** — `Conn()` dial under lock; dead seed connections invalidated on error
- **Leader lookup** — O(1) partition→leader index after metadata refresh
- **Consume timestamps** — `Record.Timestamp` from batch `firstTimestamp + delta`
- **DescribeConfigs** — legacy v1 encode/decode + version-parameterized API
- **ACL create** — response error decoding
- **ACL + CreatePartitions negotiation** — added to `ClientVersion()` map

### Changed

- `DescribeCluster` uses Metadata API (stable across 3.4–4.x)

## [0.15.0] - 2026-06-23

### Added

- **Multi-listener Docker stack** — PLAINTEXT (`9092`), SSL (`9093`), SASL_PLAINTEXT (`9094`), SASL_SSL (`9095`) on Kafka 3.9 KRaft
- **Test TLS/JAAS assets** — `scripts/gen-test-certs.sh`, `docker/secrets/`, SCRAM user bootstrap (`docker/init/init-scram-users.sh`)
- **Apicurio Schema Registry** in Docker Compose (`8081`, Confluent-compatible `/apis/ccompat/v6`)
- **Security integration tests** — PLAINTEXT, SSL, mTLS, SASL/PLAIN, SCRAM-SHA-256/512, SASL_SSL
- **Schema Registry integration test** — JSON schema register + wire encode/decode
- **`docs/CAPABILITIES.md`** — connection types, SASL mechanisms, data types, use cases, roadmap gaps
- **`SCRAMPlaintextSecurity` / `TLSOnlySecurity`** helpers
- **PBKDF2 unit test** for SCRAM salted password derivation

### Fixed

- **SCRAM-SHA-256/512** — PBKDF2 iteration used wrong HMAC key; server-first message in `error_message` field; empty `auth_bytes` on auth complete
- **SaslAuthenticate v1** — downgraded from flex header v2 (broke SASL wire)
- **TLS** — explicit handshake after TCP connect
- **Schema registry default URL** — Apicurio ccompat path

### Changed

- **GitHub Actions integration workflow** — cert generation, wait for all listeners + schema registry, per-profile broker env vars
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` in compose (tests create topics explicitly)

## [0.14.0] - 2026-06-23

### Added

- **KIP-394 consumer join flow** — automatic retry on `MEMBER_ID_REQUIRED` (error 79) with broker-assigned member id
- **`protocol.APIError`** — typed broker error codes from protocol decoders for retriable mapping

### Fixed

- **FindCoordinator v1** — decode `error_message` between error code and coordinator node
- **Fetch v11** — legacy request (`forgotten_topics_data`, `rack_id`) and response (session fields, LSO/log start, aborted transactions, preferred read replica)
- **Record batch decode** — correct magic byte / CRC / attributes header layout (fixes false gzip decompression)
- **DescribeGroups v4** — legacy encode/decode for Kafka 3.9 compatibility
- **OffsetFetch v5** — `require_stable` on request
- **InitProducerId** — retriable retry when coordinator is loading (error 14)
- **Integration tests** — `WithConsumeFromBeginning` for produce-then-consume; pause/resume deadline fix

### Changed

- Default **DescribeGroups** API version downgraded to v4 (legacy wire) for broker compatibility
- **Flex header routing** — DescribeGroups flex header only at v5+

## [0.13.0] - 2026-06-23

### Added

- **`Admin.DescribeCluster`** — cluster id, controller id, and broker registry (DescribeCluster API)
- **`WithConsumeFromBeginning`** option for consumer offset reset behavior
- **GitHub Actions integration workflow** — runs `go test -tags=integration` against Docker Kafka
- Integration tests for DescribeCluster and consumer Pause/Resume

### Fixed

- **Docker Compose** — valid KRaft `CLUSTER_ID`, listener config for local integration tests
- **Metadata v8** encode/decode (legacy broker compatibility), including `leader_epoch` in partition metadata
- **ApiVersions v2** request/response parsing (removed bogus leading int32; v3 software name/version when enabled)
- **Flexible request header v2** selection per API version (`internal/protocol/flex.go`)
- **CreateTopics / ListGroups** legacy v4/v2 response decoding for Kafka 3.9
- **DescribeCluster** request (`endpoint_type`) and response field order
- **Produce v7** legacy path with record-batch CRC32C and correct record header counts
- **Consumer Pause/Resume** integration test logic (consume first message before pausing)

## [0.12.0] - 2026-06-23

### Added

- **`Admin.IncrementalAlterTopicConfigs`** — IncrementalAlterConfigs API (SET/DELETE operations)
- **`Admin.DeleteConsumerGroups`** — DeleteGroups API via group coordinator
- **`docker-compose.yml`** — single-node KRaft Kafka for local development
- **Integration tests** (`//go:build integration`) — produce/consume/admin against live broker

### Changed

- README documents running integration tests with Docker

## [0.11.0] - 2026-06-23

### Added

- **`Admin.AlterTopicConfigs`** — AlterConfigs API for topic configuration changes
- **`Admin.CreatePartitions`** — add partitions to existing topics
- **`Admin.DeleteConsumerGroupOffsets`** — OffsetDelete API for group offset reset
- **`Consumer.Pause` / `Resume` / `PausedPartitions`** — pause fetching per partition during rebalance or maintenance

### Fixed

- **Transaction API keys** corrected: InitProducerId (22), AddPartitionsToTxn (24), EndTxn (26), OffsetDelete (47)
- **CreateTopic / DeleteTopics** decode broker error codes and return typed `KafkaError` (no longer silent success on failure)
- **Consumer.Poll** rebalance error handling indentation/braces

## [0.10.0] - 2026-06-23

### Added

- **`Cluster.RequestAny`** — tries metadata brokers then seed brokers with retry on seed failure
- **`Cluster.RequestViaSeed`** — bootstrap requests through seed connections with negotiated versions
- **`Admin.DescribeConsumerGroups`** — group state and member metadata (DescribeGroups API)
- **`Client.NegotiatedAPIVersion`** / **`NegotiatedAPIVersions`** — introspect versions negotiated at connect

### Changed

- **Admin and ACL operations** use `RequestAny` instead of hardcoded `Brokers[0]`
- **`Client.ApiVersions`** uses seed broker path with failover
- **Idempotent `InitProducerId`** (non-transactional) uses `RequestViaSeed`
- **`FindCoordinator`** uses `RequestViaSeed` with negotiated API version

## [0.9.0] - 2026-06-23

### Added

- **ApiVersions negotiation at connect** — `Cluster.NegotiateVersions` picks broker-compatible API versions automatically
- **`Cluster.NegotiatedVersion`** for introspecting negotiated protocol versions
- **`WithGroupInstanceID`** — static group membership (`group.instance.id`)
- **`ConsumerConfig.RebalanceTimeout`** — wired into JoinGroup request
- **Cooperative rebalance** — `AssignorCooperativeSticky` rejoins without `LeaveGroup` on rebalance
- **Consumer worker pool** — `Consumer.Run` respects `Concurrency.ConsumerWorkers`

### Changed

- **JoinGroup v9 wire format** — correct session/rebalance timeouts and consumer subscription metadata
- **JoinGroup rejoin** passes existing `memberID` on cooperative and eager rebalances
- **zstd compression** rejected at config validation (clear error instead of produce-time failure)

### Fixed

- **`NegotiateVersion`** no longer returns broker min version when client max is too low

## [0.8.0] - 2026-06-23

### Added

- **Transaction coordinator lookup** — `FindCoordinator` / `TransactionCoordinator` via seed brokers (not `Brokers[0]`)
- **Transactional record-batch flag** (`0x0010`) on produce within open transactions
- **`SendOffsetsToTxn`** — `AddOffsetsToTxn` + `TxnOffsetCommit` for consume-transform-produce EOS
- **`TransactionConfig.Timeout`** wired to `InitProducerId` (replaces hardcoded 60s)

### Fixed

- **Transactional produce** used shared producer `idState` instead of transaction-scoped sequences
- **Compression failures** surface as errors instead of silently sending uncompressed batches
- **Consumer coordinator lookup** uses seed broker + proper `FindCoordinator` API

## [0.7.0] - 2026-06-23

### Added

- **`AddPartitionsToTxn`** protocol encoding/decoding and automatic partition registration in `TransactionalProducer`
- **`ProduceWithinTxnResult`** — transactional produce with broker offset delivery
- **`WithAutoCommit`** option for explicit auto-commit configuration in `Consumer.Run`

### Changed

- **Transactional produce** uses dedicated transaction PID/sequence state instead of the shared non-transactional producer
- README examples corrected (imports, async delivery pattern, AutoCommit default, seek vs Run)

## [0.6.0] - 2026-06-23

### Added

- **`ProduceSyncResult`** — returns broker topic/partition/offset per record on successful produce
- **`ProduceRecordResult`** delivery type; async `ProduceResult.Result` includes offset
- **Sequence reserve/rollback** — idempotent sequences no longer advance on failed send attempts
- **`ErrInvalidProducerConfig`** — idempotent producer requires `acks=all`
- **`ErrCodeInvalidProducerEpoch`**, **`ErrCodeOutOfOrderSequence`** with retriable handling and automatic PID reset

### Changed

- **`Client.Producer()`** — single shared producer per client (one idempotent PID/sequence state)
- **AsyncProducer** — workers use shared producer; delivery reports include offsets
- **`Consumer.Run`** — commits offsets **after** successful handler completion (not before)
- Multi-broker produce batches roll back sequences only for failed broker partitions

### Fixed

- Idempotent produce could emit duplicate sequence numbers on retriable retry
- Async producer workers each created separate producers with independent sequence state

## [0.5.0] - 2026-06-23

### Added

- **RebalanceListener** — `OnPartitionsAssigned` / `OnPartitionsRevoked` callbacks (Java `ConsumerRebalanceListener` equivalent)
- **AssignorCooperativeSticky** assignor name support
- Automatic rebalance on `REBALANCE_IN_PROGRESS` fetch errors via `Consumer.Rebalance()`
- **Snappy** and **LZ4** compression codecs (pure Go, no external deps)
- **ACL admin** — `CreateACLs`, `DescribeACLs`, `DeleteACLs`
- **log/slog adapter** — `WithSlogLogger` for stdlib structured logging
- **ApiVersions** API — `Client.ApiVersions()`, `SupportsAPI()`
- Compression codec constants: `CompressionSnappy`, `CompressionLZ4`, `CompressionZstd`

### Changed

- Fetch and produce paths use unified `compress.Compress` / `Decompress` for all codecs
- Partition assignment callbacks fire after committed offsets are loaded

## [0.4.0] - 2026-06-23

### Added

- **`observe` package** — native structured logging, metrics, and tracing hooks
- **Log formats**: text, JSON, and **ECS** (Elastic Common Schema) for Elasticsearch / Elastic APM
- **Prometheus exposition** — `Client.PrometheusHandler()`, `WritePrometheus()` (no client_golang dependency)
- **OpenTelemetry bridges** — `RegisterOTelBridge`, `OTelBridge`, `PrometheusRecorder` for wiring external SDKs without gokafka deps
- **Elastic APM logger** — `ElasticAPMLogger` ECS JSON adapter
- **Enhanced metrics** — broker request latency, request/error counters, hook registration
- **Distributed trace context** — `trace.id` / `span.id` propagation in logs (OTel compatible field names)
- **Structured errors** — `ErrorObject`, `ErrorJSON`, `KafkaError.ErrorDetail()` for log/APM pipelines
- **Span instrumentation** on connect, produce, and consumer join paths
- Observability options: `WithObservability`, `WithLogLevel`, `WithLogFormat`, `WithLogger`, `WithTracer`, `WithMetricsHook`

### Changed

- `metrics` package is now a thin alias over `observe.Collector` (backward compatible)
- Client uses `observe.Hub` for unified logging, metrics, and tracing

## [0.3.0] - 2026-06-23

### Added

- **ConnectionConfig** — dial/request timeouts, advertised-listener host remapping (`WithBrokerHostRemap`, `BrokerAddressMapper`)
- **BatchProducer** — respects `BatchSize` and `Linger` from producer config
- **Idempotent produce** — InitProducerId, producer epoch, and per-partition sequence numbers on the wire
- **OffsetFetch** on consumer join — resume from committed group offsets
- **Seek**, **SeekToBeginning**, **SeekToEnd** via ListOffsets API
- **Partition assignment parsing** from SyncGroup response (range/roundrobin/sticky assignor names)
- **Consumer assignors** — `AssignorRange`, `AssignorRoundRobin`, `AssignorSticky`
- **read_committed** fetch isolation level for transactional topics
- **Record headers** on produce and consume paths
- **gzip compression** applied on the wire (not just attribute byte)
- **Fetch gzip decompression** when reading compressed batches
- **Retriable-aware retries** — `IsRetriable`, `AsKafkaError`; producer retries only retriable broker errors
- **Admin**: `ListConsumerGroups`, `DescribeTopicConfigs`, `DescribeTopic` (leaders, ISR, replicas)
- **AssignedPartitions** consumer introspection API
- Connection invalidation and reconnect on broker request failure

### Changed

- Produce request encoding uses proper record batch v2 format with configurable acks
- `EncodeInitProducerID` accepts optional transactional id pointer
- AutoCommit in `Consumer.Run` now commits when enabled (was inverted)
- Broker cluster accepts `Options` for networking configuration

### Fixed

- JoinGroup returns error when broker responds with non-zero error code
- Consumer no longer assigns all partitions ignoring coordinator assignment when present

## [0.2.0] - 2026-06-23

### Added

- Functional options API: `NewConfig` + `Option` helpers
- Async producer with worker pool
- `Consumer.Run` with heartbeat and `Leave`
- Transactional producer skeleton
- SASL SCRAM-SHA-256/512
- Partition strategies, typed payloads, structured errors
- Unit tests for partitioners and config validation

## [0.1.0] - 2026-06-23

### Added

- Initial pure Go Kafka 4+ client with zero third-party dependencies

[Unreleased]: https://github.com/sinamohsenifar/gokafka/compare/v0.17.0...HEAD
[0.17.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/sinamohsenifar/gokafka/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/sinamohsenifar/gokafka/releases/tag/v0.1.0
