---
title: Share coordinator & __share_group_state
type: concept
category: Concepts
subcategory: KIP-932
status: reference
tags: [gokafka, concept, kip-932]
updated: 2026-06-30
---

# Share coordinator & `__share_group_state`

Where [[Research: KIP-932 share groups (Queues for Kafka)|KIP-932]] durably keeps per-record delivery state — a new piece of broker infrastructure (Source: [[sources/apache-kip-932]]).

- **Share Coordinator** — distributed across brokers (like the group coordinator). The broker leading a partition of the state topic coordinates the share-partitions whose records hash there.
- **`__share_group_state`** — new internal topic holding the state, as two record types:
  - **ShareSnapshot** — full durable snapshot (SPSO, per-record states, delivery counts).
  - **ShareUpdate** — incremental change referencing the latest snapshot epoch.
  - Epochs: `StateEpoch` (init), `LeaderEpoch` (fences zombie leaders), `SnapshotEpoch` (lineage).
- **Inter-broker RPCs:** `InitializeShareGroupState`, `ReadShareGroupState`, `WriteShareGroupState`, `DeleteShareGroupState`.
- **Offsets:** SPSO (start) … SPEO (end) bound the in-flight window; capped by `group.share.partition.max.record.locks`.
- A share-partition leader, on its first `ShareFetch`, resolves its coordinator via `FindCoordinator(key_type=SHARE)`, then `ReadShareGroupState` to load in-memory state; acks trigger `WriteShareGroupState` (blocks until replicated).

This is **server-side** machinery — a [[packages/kfake-mock-broker|client]] like GoKafka only speaks the four client-facing RPCs (76–79), not these inter-broker ones.

## Related
[[concepts/share-group-acquisition-lock|Share-group acquisition lock & delivery count]] · [[sources/apache-kip-932|Apache cwiki KIP-932]] · [[features/share-groups|Share groups (KIP-932)]] · [[Research: KIP-932 share groups (Queues for Kafka)]] · [[Audit: KIP-932 implementation gaps]] · [[architecture/cluster-coordinator|Cluster: metadata, leaders, coordinators]] · [[concepts/server-side-assignor|Server-side vs client-side assignment]]
