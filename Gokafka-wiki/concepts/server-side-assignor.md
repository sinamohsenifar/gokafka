---
title: Server-side vs client-side assignment
type: concept
category: Concepts
subcategory: KIP-848
status: reference
tags: [gokafka, concept, kip-848]
updated: 2026-06-30
---

# Server-side vs client-side assignment

[[Research: KIP-848 next-gen consumer rebalance protocol|KIP-848]] moves partition assignment from the client to the **broker** by default (Source: [[sources/apache-kip-848]], [[sources/kafka-docs-rebalance-protocol]]).

- **Server-side (default, GA in Kafka 4.0):** the group coordinator runs a pluggable assignor. Broker config `group.consumer.assignors` (defaults `uniform`, `range`); a client may request one via `group.remote.assignor`.
- **Client-side (for Kafka Streams):** the coordinator nominates a member via the heartbeat's `ShouldComputeAssignment` flag; that member calls `ConsumerGroupPrepareAssignment` (fetch group state) then `ConsumerGroupInstallAssignment` (submit result). **Not yet implemented** as of mid-2026 (KAFKA-18327).

Migration for apps that used a client-side assignor: port it to a broker-side assignor implementing `org.apache.kafka.coordinator.group.api.assignor.ConsumerGroupPartitionAssignor`.

> [!gap] Rack-aware assignment is "not fully supported" (KAFKA-17747).

**In GoKafka:** uses **server-side** assignment exclusively — no client-side assignor path needed, which keeps the client thin. See [[features/next-gen-groups]].

## Related
- [[concepts/consumergroupheartbeat|ConsumerGroupHeartbeat (RPC)]] · [[concepts/epoch-reconciliation|Epoch reconciliation]] · [[sources/kafka-docs-rebalance-protocol|Apache Kafka 4.1 docs rebalance protocol]]
- [[features/next-gen-groups|Next-gen consumer groups (KIP-848)]] · [[sources/apache-kip-848|Apache cwiki KIP-848]] · [[sources/confluent-kip-848-blog|Confluent KIP-848 blog]]
- [[Research: KIP-848 next-gen consumer rebalance protocol]] · [[packages/consumer|Consumer & groups]]
