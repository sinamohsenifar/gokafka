---
title: ConsumerGroupHeartbeat (RPC)
type: concept
category: Concepts
subcategory: KIP-848
status: reference
tags: [gokafka, concept, kip-848]
updated: 2026-06-30
---

# ConsumerGroupHeartbeat (RPC)

The single RPC at the heart of [[Research: KIP-848 next-gen consumer rebalance protocol|KIP-848]]. It **replaces JoinGroup + SyncGroup + Heartbeat** (three RPCs) with one continuous heartbeat (Source: [[sources/apache-kip-848]]).

- **Request:** `GroupId`, `MemberId` (server-generated UUID), `MemberEpoch`, `SubscribedTopicNames`/`SubscribedTopicRegex`, `ServerAssignor` or `ClientAssignors[]`, owned `TopicPartitions`.
- **Response:** `MemberId`, `MemberEpoch`, `HeartbeatIntervalMs`, `Assignment` (assigned + pending partitions), `ShouldComputeAssignment`.
- **Errors:** `FENCED_MEMBER_EPOCH`, `UNSUPPORTED_ASSIGNOR`, `UNRELEASED_INSTANCE_ID`, `INVALID_REGULAR_EXPRESSION`.

The member **declares** its desired state (subscriptions, owned partitions) and **acknowledges** what the coordinator returns; the coordinator drives reconciliation via [[concepts/epoch-reconciliation|epochs]]. No global barrier — a member pauses only the partitions it is told to revoke.

**In GoKafka:** API key 68; paired with `ConsumerGroupDescribe` (69). See [[features/next-gen-groups]].

## Related
[[concepts/epoch-reconciliation]] · [[concepts/server-side-assignor]] · [[features/next-gen-groups]] · [[sources/apache-kip-848]] · [[sources/kafka-docs-rebalance-protocol]] · [[Research: KIP-848 next-gen consumer rebalance protocol]] · [[packages/consumer|Consumer & groups]]
