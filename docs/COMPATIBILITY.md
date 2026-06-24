# Compatibility matrix

## Go versions

| Go version | Status |
|------------|--------|
| **1.22** | Minimum (`go.mod`) |
| **1.22 – 1.24** | Tested in CI on every PR |
| **1.25+** | Supported when released; add to CI matrix |

## Apache Kafka broker versions

Per [Apache Kafka downloads](https://kafka.apache.org/community/downloads/):

| Release | Docker image | CI |
|---------|--------------|-----|
| **3.9.2** | `apache/kafka:3.9.2` | Compatibility matrix |
| **4.1.2** | `apache/kafka:4.1.2` | Primary integration (every PR) |
| **4.2.1** | `apache/kafka:4.2.1` | Scheduled matrix |
| **4.3.0** | `apache/kafka:4.3.0` | Compatibility matrix (every PR) |

**Minimum broker:** 3.4+ (KRaft or ZooKeeper-era protocol baseline).  
**Kafka 4.x:** KRaft only; pre-2.1 client protocol removed (KIP-896). GoKafka negotiates API versions ≥2.1-era wire.

## Client protocol model

1. `ApiVersions` negotiation on connect
2. Flex request headers when required (`internal/protocol/flex.go`)
3. Conservative `Ver*` caps in `internal/protocol/keys.go` for stable wire on 3.9–4.3

## Schema Registry

- Confluent-compatible REST (Apicurio ccompat v6 in docker-compose)
- Wire format: magic `0x00` + big-endian schema ID (+ Protobuf message indexes when applicable)

## Not supported

- ZooKeeper-mode brokers on Kafka 4.x (brokers removed ZK)
- Pre-2.1 Kafka protocol clients (not applicable — GoKafka is a native implementation)

**KIP-848** requires `group.coordinator.rebalance.protocol=consumer` on the broker (set in docker-compose).  
**KIP-932** requires Kafka 4.1+ with `share.version=1` (`kafka-features.sh upgrade --feature share.version=1`; enabled automatically in `kafka-init`).
