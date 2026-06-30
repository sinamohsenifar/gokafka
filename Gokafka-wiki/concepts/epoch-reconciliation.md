---
title: Epoch reconciliation (group / assignment / member)
type: concept
category: Concepts
subcategory: KIP-848
status: reference
tags: [gokafka, concept, kip-848]
updated: 2026-06-30
---

# Epoch reconciliation

How [[Research: KIP-848 next-gen consumer rebalance protocol|KIP-848]] converges a group **incrementally**, without a stop-the-world barrier (Source: [[sources/apache-kip-848]]).

Three epochs:
- **Group Epoch** — bumped whenever subscriptions, topic metadata, or membership change.
- **Assignment Epoch** — set when the group coordinator computes a new target assignment.
- **Member Epoch** — each member's current position; drives its incremental convergence toward the target.

Members converge **independently**; the coordinator resolves dependencies — e.g. a partition is only assigned to a new owner **after** the previous owner revokes it ("new partitions are incrementally assigned … when they are revoked by the other members"). A stale member is fenced with `FENCED_MEMBER_EPOCH` / `STALE_MEMBER_EPOCH` (the latter on OffsetCommit/Fetch v9).

This replaces the classic protocol's single generation id + global JoinGroup/SyncGroup round.

## Related

[[concepts/consumergroupheartbeat|ConsumerGroupHeartbeat (RPC)]] · [[concepts/server-side-assignor|Server-side vs client-side assignment]] · [[sources/apache-kip-848|Apache cwiki KIP-848]] · [[sources/kafka-docs-rebalance-protocol|Apache Kafka 4.1 docs rebalance protocol]] · [[features/next-gen-groups|Next-gen consumer groups (KIP-848)]] · [[Research: KIP-848 next-gen consumer rebalance protocol]]
