---
title: Share groups (KIP-932)
type: feature
tags: [gokafka, share-groups, kip-932]
updated: 2026-06-30
---

# Share groups (KIP-932)

Queue semantics over Kafka. `Client.ShareConsumer(topics)` → `ShareConsumer` with `Poll` (acquire) and `Acknowledge` (Accept / Release / Reject), plus **Renew** via ShareAcknowledge v2 (KIP-1222). Uses ShareGroupHeartbeat (76), ShareGroupDescribe (77), ShareFetch (78), ShareAcknowledge (79).

Requires Kafka 4.1+ with `share.version=1`. Not yet supported by [[compatibility/redpanda|Redpanda]] (auto-skipped). Among Go clients, only GoKafka and franz-go implement it.

## Related
- [[packages/consumer]] · [[protocol/kip-coverage]] · [[competitors/parity-matrix]]
