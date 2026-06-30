---
title: Consumer lag
type: feature
category: Features
subcategory: Observability
status: stable
tags: [gokafka, feature, consumer, lag]
updated: 2026-06-30
---

# Consumer lag

`Admin.ConsumerGroupLag(ctx, group)` returns per-partition `PartitionLag{Topic, Partition, Committed, LogEndOffset, Lag}` — the gap between each partition's log-end offset and the group's committed offset.

Built from OffsetFetch (committed) + ListOffsets latest, grouping ListOffsets by partition leader with metadata-refresh retries. The headline monitoring primitive franz-go's `kadm` exposes and that sarama/kafka-go users hand-roll.

## Related
[[packages/admin]] · [[packages/consumer]] · [[packages/observability]] · [[competitors/parity-matrix]] · [[competitors/franz-go]] · [[competitors/sarama]]
