---
title: "Apache cwiki — KIP-932: Queues for Kafka (share groups)"
type: source
category: Research
subcategory: KIP-932
status: reference
tags: [gokafka, source, kip-932, research]
updated: 2026-06-30
url: https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka
---

# Apache cwiki — KIP-932: Queues for Kafka

Primary design document. **Confidence: high.**

## Key claims
- **Share groups** = cooperative consumption: many consumers read the **same** partitions; "the number of consumers in a share group can exceed the number of partitions." Trades strict ordering for elasticity — records "can be delivered out of order … when redeliveries occur."
- **Three new RPCs:** `ShareGroupHeartbeat` (membership/session; no fencing rebalance), `ShareFetch` (fetch + optionally piggyback acknowledgements; stateful **share sessions** with epoch modes: full=0, incremental=1..MAX, close=−1), `ShareAcknowledge` (explicit ack; validates records are in **Acquired** state).
- **Per-record acquisition-lock state machine** — states **Available → Acquired → Acknowledged / Archived**:
  - Lock default **30s** (`share.record.lock.duration.ms`); on expiry the record returns to **Available** if delivery count < limit.
  - Actions: **Accept** → Acknowledged; **Release** → Available (retry); **Reject** → Archived (no retry; poison messages).
  - **`group.share.delivery.attempt.limit`** (default **5**) → once hit, the record is **Archived** (prevents poison-loops).
- **Offsets:** Share-Partition Start Offset (**SPSO**) lower bound, Share-Partition End Offset (**SPEO**) upper bound; gap capped by `group.share.partition.max.record.locks`.
- **State persistence:** new internal topic **`__share_group_state`** managed by a distributed **Share Coordinator** via inter-broker RPCs `InitializeShareGroupState`, `ReadShareGroupState`, `WriteShareGroupState`, `DeleteShareGroupState`; record types **ShareSnapshot** (full) + **ShareUpdate** (incremental), with StateEpoch/LeaderEpoch/SnapshotEpoch. The share-partition leader locates its coordinator via `FindCoordinator(key="groupId:topicId:partition", key_type=SHARE)`.
- **Configs:** `share.isolation.level` (read_uncommitted/read_committed), `share.auto.offset.reset`, `share.acknowledgement.mode` (implicit default / explicit), `group.share.heartbeat.interval.ms`, `group.share.session.timeout.ms`. Gated by the `share.version` feature.

## Relevance to GoKafka
GoKafka's `ShareConsumer` implements the client side: APIs 76 ShareGroupHeartbeat, 77 ShareGroupDescribe, 78 ShareFetch, 79 ShareAcknowledge — with Accept/Release/Reject and **Renew** (ShareAcknowledge v2 = KIP-1222). See [[features/share-groups]].

## Related
- [[Research: KIP-932 share groups (Queues for Kafka)]] · [[features/share-groups|Share groups (KIP-932)]] · [[concepts/share-group-acquisition-lock|Share-group acquisition lock & delivery count]]
- [[concepts/share-coordinator-state|Share coordinator & __share_group_state]] · [[sources/confluent-share-consumer-ga|Confluent share consumer GA]] · [[Audit: KIP-932 implementation gaps|Audit KIP-932 gaps]]
