---
title: "Apache cwiki — KIP-848: The Next Generation of the Consumer Rebalance Protocol"
type: source
category: Research
subcategory: KIP-848
status: reference
tags: [gokafka, source, kip-848, research]
updated: 2026-06-30
url: https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol
---

# Apache cwiki — KIP-848 (primary spec)

The authoritative design document. **Confidence: high** (primary source).

## Key claims
- The **old** protocol's faults: most rebalance bugs needed *client-side* fixes (slow adoption); a **global synchronization barrier** means one misbehaving consumer disturbs the whole group; accumulated extensions (KIP-429 cooperative, KIP-345 static membership) made it hard to maintain; clients managed metadata independently → inconsistent group views.
- **`ConsumerGroupHeartbeat`** replaces JoinGroup + SyncGroup + Heartbeat with one RPC: members send subscriptions, owned partitions, assignor metadata; the coordinator responds with assignments and revocation instructions.
- **Three-epoch reconciliation:** *Group Epoch* (bumped on subscription/metadata/membership change), *Assignment Epoch* (when the coordinator computes a target assignment), *Member Epoch* (per-member state driving incremental convergence). Members converge independently; the coordinator resolves dependencies (revoke before reassign).
- **Assignment modes:** server-side (default; pluggable `RangeAssignor`/`UniformAssignor`) and client-side (for Kafka Streams; coordinator picks a member via `ShouldComputeAssignment`, then `ConsumerGroupPrepareAssignment` + `ConsumerGroupInstallAssignment`).
- **State** persists in `__consumer_offsets` as compactable records: `ConsumerGroupMetadataKey/Value`, `ConsumerGroupPartitionMetadataKey/Value`, `ConsumerGroupMemberMetadataKey/Value` (X members → X+2 records).
- **New/changed APIs:** `ConsumerGroupHeartbeat`, `ConsumerGroupDescribe`; `OffsetCommit`/`OffsetFetch` v9 add topic IDs + member-epoch verification (`STALE_MEMBER_EPOCH`); a `GROUP` config resource (type 16) for per-group heartbeat/session overrides.
- **Compatibility:** gated by `group.version`. A classic group auto-converts to a consumer group when the first new-protocol member joins. As of mid-2026: server-side assignors GA in **Kafka 4.0**, client-side **not implemented**, offset topic-IDs in **4.2**.

## Heartbeat RPC fields (quoted)
- Request: `GroupId`, `MemberId` (server-generated UUID), `MemberEpoch`, `SubscribedTopicNames`/`SubscribedTopicRegex`, `ServerAssignor` or `ClientAssignors[]`, `TopicPartitions` (owned).
- Response: `MemberId`, `MemberEpoch`, `HeartbeatIntervalMs`, `Assignment`, `ShouldComputeAssignment`.
- Errors: `FENCED_MEMBER_EPOCH`, `UNSUPPORTED_ASSIGNOR`, `UNRELEASED_INSTANCE_ID`, `INVALID_REGULAR_EXPRESSION`.

## Relevance to GoKafka
GoKafka implements the client side of this (ConsumerGroupHeartbeat API 68, ConsumerGroupDescribe 69) — see [[features/next-gen-groups|next-gen groups]]. It uses **server-side** assignment (no client-side assignor needed) and supports RE2J regex subscriptions.

## Related
[[Research: KIP-848 next-gen consumer rebalance protocol]] · [[concepts/consumergroupheartbeat|ConsumerGroupHeartbeat (RPC)]] · [[concepts/epoch-reconciliation|Epoch reconciliation]] · [[concepts/server-side-assignor|Server-side vs client-side assignment]] · [[features/next-gen-groups|Next-gen consumer groups (KIP-848)]] · [[sources/kafka-docs-rebalance-protocol|Apache Kafka 4.1 docs rebalance protocol]] · [[sources/confluent-kip-848-blog|Confluent KIP-848 blog]]
