---
title: "Cluster: metadata, leaders, coordinators"
type: architecture
tags: [gokafka, architecture, broker]
updated: 2026-06-30
---

# Cluster: metadata, leaders, coordinators

`internal/broker/cluster.go` — the routing brain.

- **Metadata cache** — topics, partitions, leaders, leader-epochs; refreshed on staleness and on retriable errors (NOT_LEADER, UNKNOWN_TOPIC_ID, …).
- **Leader resolution** — `LeaderBroker(topic, partition)`, `LeaderNodeID`, leader-epoch index for [[protocol/kip-coverage|KIP-320]] fencing.
- **Coordinator resolution** — `TransactionCoordinator`, group coordinator via FindCoordinator v3.
- **Version negotiation** — stores `apiVersions` map; `NegotiatedVersion`, `AdvertisesAPI` ([[compatibility/broker-quirks|v0-API + unsupported-API handling]]).
- **Routing** — `Request(node, …)`, `RequestAny` (controller-forwarded, with partial-result "shard errors" for fan-out ops like `DescribeLogDirs`), `RequestViaSeed`.

## Related
- [[client-lifecycle]] · [[protocol/version-negotiation]] · [[transport]]
