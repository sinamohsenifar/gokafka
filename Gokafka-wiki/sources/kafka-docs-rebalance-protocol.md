---
title: "Apache Kafka 4.1 docs — Consumer Rebalance Protocol"
type: source
category: Research
subcategory: KIP-848
status: reference
tags: [gokafka, source, kip-848, research]
updated: 2026-06-30
url: https://kafka.apache.org/41/operations/consumer-rebalance-protocol/
---

# Apache Kafka 4.1 docs — Consumer Rebalance Protocol

Canonical operator-facing reference. **Confidence: high** (official docs).

## Key claims (configs quoted)
- **Opt in (client):** `group.protocol=consumer` (use/omit `classic` for legacy).
- **Server feature flag:** `group.version` (auto-enabled in Kafka 4.0+).
- **Session mgmt moved server-side:** `group.consumer.heartbeat.interval.ms`, `group.consumer.session.timeout.ms` — these replace the client `heartbeat.interval.ms` / `session.timeout.ms`.
- **Assignors:** broker `group.consumer.assignors` (defaults: `uniform`, `range`); client override `group.remote.assignor`.
- **Upgrade — offline:** stop all consumers, restart with `group.protocol=consumer`; the group auto-converts when empty. **Online:** rolling restart; the group transitions Classic→Consumer when the first new-protocol member joins, interoperating until all classic members leave.
- **Limitations:** "Client-side assignors are not supported" (KAFKA-18327); "Rack-aware assignment strategies are not fully supported" (KAFKA-17747).
- **Unavailable with the new protocol (client):** `heartbeat.interval.ms`, `session.timeout.ms`, `partition.assignment.strategy`, `enforceRebalance()`.

## Related
[[sources/apache-kip-848]] · [[Research: KIP-848 next-gen consumer rebalance protocol]] · [[features/next-gen-groups|Next-gen consumer groups (KIP-848)]] · [[concepts/server-side-assignor|Server-side vs client-side assignment]] · [[concepts/epoch-reconciliation|Epoch reconciliation]] · [[concepts/consumergroupheartbeat|ConsumerGroupHeartbeat (RPC)]]
