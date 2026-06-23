# Kafka Version Compatibility

GoKafka targets **Apache Kafka 3.4+** through the latest 3.9.x / 4.x releases using negotiated API versions.

## Tested brokers

| Broker image | Notes |
|--------------|-------|
| `apache/kafka:3.9.2` | Primary CI / docker-compose stack |
| KRaft single-node | PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL listeners |

## Compatibility model

1. **ApiVersions negotiation** on connect — broker max is clamped to client-implemented max per API key (`internal/protocol/versions.go`).
2. **Request headers** use negotiated version + flex header v2 when required (`internal/protocol/flex.go`).
3. **Request bodies** follow negotiated version; conservative caps live in `internal/protocol/keys.go`.
4. **DescribeConfigs** uses v4 when the broker supports it.

## API coverage matrix (client max)

| API | Client max | Kafka 3.4 | Kafka 3.9 | Notes |
|-----|------------|-----------|-----------|-------|
| Produce | 9 | yes | yes | Flex v9+ when broker supports |
| Fetch | 12 | yes | yes | Flex v12+ when broker supports |
| Metadata | 12 | yes | yes | Topic UUIDs (v10+) |
| JoinGroup | 6 | yes | yes | Flex v6+ |
| OffsetCommit | 8 | yes | yes | Flex v8+ |
| DescribeCluster | 1 | 3.7+ | yes | Wire API 60 with metadata fallback |
| CreateTopics | 4 | yes | yes | `TopicSpec` with configs |
| DescribeConfigs | 4 | yes | yes | Topic + broker configs |
| CreatePartitions | 2 | yes | yes | Flex v2 |
| SASL SCRAM | 1 | yes | yes | SHA-256 / SHA-512 |
| ACLs | 1 | yes | yes | Create/Describe/Delete v1 |
| IncrementalAlterConfigs | 0 | yes | yes | v0 wire |
| Transactions | 2+ | yes | yes | Idempotent producer default |

## Roadmap

- Flex Produce v9+, Fetch v12+ where brokers require them
- DescribeCluster wire API (v1) — metadata fallback today
- GSSAPI/Kerberos SASL — see `docs/GSSAPI.md`
- zstd compression — see `docs/ZSTD.md`
