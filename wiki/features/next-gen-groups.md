---
title: Next-gen consumer groups (KIP-848)
type: feature
tags: [gokafka, kip-848, consumer]
updated: 2026-06-30
---

# Next-gen consumer groups (KIP-848)

The new server-driven rebalance protocol (`group.protocol=consumer`) via `ConsumerGroupHeartbeat` (68) + `ConsumerGroupDescribe` (69). The broker computes assignments; the client sends incremental heartbeats — no client-side JoinGroup/SyncGroup dance.

Includes **RE2J server-side regex subscriptions** — `Client.ConsumerPattern(regex)` sends `SubscribedTopicRegex` and the broker resolves matching topics.

Not yet supported by [[compatibility/redpanda|Redpanda v26.1]] (auto-skipped).

## Related
- [[packages/consumer]] · [[protocol/kip-coverage]]
