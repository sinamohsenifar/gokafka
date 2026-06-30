---
title: Partitioners
type: package
category: Packages
subcategory: Producer
status: stable
tags: [gokafka, package, producer, partitioner]
updated: 2026-06-30
---

# Partitioners

Root `partitioner.go`. The `Partitioner` interface; explicit `Record.Partition` always wins.

- **`HashPartitioner`** (default) — murmur2, **Java-client/Sarama-compatible** (unit-pinned to the Apache `UtilsTest.testMurmur2` vectors).
- **`CRC32Partitioner`** — CRC32 (IEEE), **librdkafka/kafka-go-compatible** for mixed-fleet co-partitioning.
- **`RoundRobinPartitioner`** — spreads keyless records.
- **`WithPartitioner(p)`** — any custom implementation.

Cross-client co-partitioning is the point: the same key lands on the same partition as Java/librdkafka producers.

## Related
[[packages/producer]] · [[competitors/parity-matrix]] · [[competitors/sarama]] · [[competitors/kafka-go]] · [[architecture/cluster-coordinator]] · [[packages/compression]]
