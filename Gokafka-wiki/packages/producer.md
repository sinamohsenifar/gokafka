---
title: Producer
type: package
category: Packages
subcategory: Producer
status: stable
tags: [gokafka, package, producer]
updated: 2026-06-30
---

# Producer

Root `producer.go`. Sync (`ProduceSync`, `ProduceSyncResult`) and batched produce; idempotent by default (InitProducerID + sequence numbers); transactional via [[packages/transactions|BeginTransaction]].

- **Partitioning** — `Record.Partition` (explicit) wins; otherwise the configured [[packages/partitioners|Partitioner]] (murmur2 default, CRC32, round-robin, or custom).
- **Batching** — `internal/produce` builds v2 record batches; **Produce v12** (enables [[features/exactly-once-tv2|TV2]] implicit partition-add + KIP-951 leader hints).
- **Compression** — [[packages/compression|gzip/snappy/lz4/zstd]], all pure Go.
- Errors surface as `*KafkaError` with retriability; `TRANSACTION_ABORTABLE` (120) is non-retriable.

## Related
- [[packages/partitioners]] · [[packages/transactions]] · [[packages/compression]]
- [[features/exactly-once-tv2|Exactly-once / KIP-890 TV2]] · [[packages/admin|Admin]] · [[architecture/transport|Transport: framing & connections]]
- [[competitors/parity-matrix|Competitor parity matrix]] · [[protocol/api-coverage|API coverage]]
