---
title: Share groups (KIP-932)
type: feature
category: Features
subcategory: Groups
status: ga
tags: [gokafka, feature, kip-932, share-groups]
updated: 2026-06-30
---

# Share groups (KIP-932)

Queue semantics over Kafka. `Client.ShareConsumer(topics)` → `ShareConsumer` with `Poll` (acquire) and `Acknowledge` (Accept / Release / Reject), plus **Renew** via ShareAcknowledge v2 (KIP-1222). Uses ShareGroupHeartbeat (76), ShareGroupDescribe (77), ShareFetch (78), ShareAcknowledge (79).

Requires Kafka 4.1+ with `share.version=1`. Not yet supported by [[compatibility/redpanda|Redpanda]] (auto-skipped). Among Go clients, only GoKafka and franz-go implement it.

## Related
- 🔬 Deep dive: [[Research: KIP-932 share groups (Queues for Kafka)]] · concepts: [[concepts/share-group-acquisition-lock|share-group-acquisition-lock]] · [[concepts/share-coordinator-state|share-coordinator-state]]
- [[packages/consumer]] · [[protocol/kip-coverage]] · [[competitors/parity-matrix]]
- [[sources/apache-kip-932]] · [[features/next-gen-groups]] · [[Audit: KIP-932 implementation gaps]]
