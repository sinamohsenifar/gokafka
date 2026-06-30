---
title: kfake — in-process mock broker
type: package
category: Packages
subcategory: Testing
status: stable
tags: [gokafka, package, testing]
updated: 2026-06-30
---

# kfake — in-process mock broker

`github.com/sinamohsenifar/gokafka/kfake` is a **pure-Go, in-memory Kafka broker** for tests. It speaks the wire protocol at the exact API versions the GoKafka client negotiates, so producer/consumer/admin code can be unit-tested against the **real client** with **no Docker or cluster**.

```go
b, _ := kfake.NewBroker()
defer b.Close()
b.AddTopic("events", 1)
cfg, _ := gokafka.NewConfig([]string{b.Addr()})
client, _ := gokafka.NewClient(cfg) // produce/consume/commit/admin/lag all work
```

## Coverage
Connect (ApiVersions v3, Metadata v12), admin (CreateTopics v4, DeleteTopics v6), idempotent produce (InitProducerID + Produce v9), ListOffsets v3, Fetch v12, single-member consumer groups (FindCoordinator v3, Join v6 / Sync v5 / Heartbeat v4 / Leave v5), and OffsetCommit v8 + OffsetFetch v7 (single) / v8 (multi-group, so [[features/consumer-lag|ConsumerGroupLag]] works).

## Key techniques
- **Advertise narrow ApiVersions ranges** so the client negotiates *down* to the versions kfake implements.
- **Match the client's decoder, not raw Kafka** — kfake encodes exactly what the GoKafka decoders read.
- **Store record batches opaquely** — patch `base_offset` in bytes `[0:8]`, read `records_count` at `[57:61]`, serve verbatim on Fetch. Never parses individual records.
- **Single-member groups** — the broker echoes the leader-computed SyncGroup assignment back.
- Reproduces the **KIP-511** non-flexible ApiVersions response header (see [[protocol/flexible-encoding]]).

## The oracle insight
The **real GoKafka client is the correctness oracle** — tests drive full produce→consume→commit→lag and admin flows end-to-end against the mock. If kfake's wire format were wrong, the real client's decoders would reject it. See [[decisions/adr-mock-as-oracle]].

Files: `kfake/{broker,store,apis,admin,produce,initproducer,listoffsets,fetch,groups}.go`.

## Comparison
Parity with franz-go's `kfake`, sarama's `mocks`, confluent's mock cluster — see [[competitors/parity-matrix]]. For schema tests, `schema.MockRegistry` is the equivalent in-memory Schema Registry.

## Related
[[decisions/adr-mock-as-oracle]] · [[protocol/flexible-encoding]] · [[protocol/version-negotiation]] · [[competitors/parity-matrix]] · [[packages/producer]] · [[packages/consumer]] · [[architecture/wire-protocol]]
