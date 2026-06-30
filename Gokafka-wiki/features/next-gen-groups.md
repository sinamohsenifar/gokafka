---
title: Next-gen consumer groups (KIP-848)
type: feature
category: Features
subcategory: Groups
status: ga
tags: [gokafka, feature, kip-848, consumer]
updated: 2026-06-30
---

# Next-gen consumer groups (KIP-848)

The new server-driven rebalance protocol (`group.protocol=consumer`) via `ConsumerGroupHeartbeat` (68) + `ConsumerGroupDescribe` (69). The broker computes assignments; the client sends incremental heartbeats — no client-side JoinGroup/SyncGroup dance.

Includes **RE2J server-side regex subscriptions** — `Client.ConsumerPattern(regex)` sends `SubscribedTopicRegex` and the broker resolves matching topics.

Not yet supported by [[compatibility/redpanda|Redpanda v26.1]] (auto-skipped).

## Related
- 🔬 Deep dive: [[Research: KIP-848 next-gen consumer rebalance protocol]] · concepts: [[concepts/consumergroupheartbeat]] · [[concepts/epoch-reconciliation]] · [[concepts/server-side-assignor]]
- [[packages/consumer]] · [[protocol/kip-coverage]] · [[sources/apache-kip-848]] · [[features/share-groups]] · [[concepts/server-side-assignor]]
