---
title: Consumer & groups
type: package
tags: [gokafka, consumer]
updated: 2026-06-30
---

# Consumer & groups

Root `consumer.go`, `consumer848.go`, `share_consumer.go`, `offset.go`.

- **Classic groups** — JoinGroup/SyncGroup/Heartbeat/LeaveGroup; assignors: range, round-robin, sticky, **cooperative-sticky** (KIP-429).
- **Next-gen groups** — [[features/next-gen-groups|KIP-848]] `ConsumerGroupHeartbeat`, incl. RE2J server-side regex subscriptions (`ConsumerPattern`).
- **Share groups** — [[features/share-groups|KIP-932]] queue semantics (`ShareConsumer`).
- **Offsets** — manual `Commit`; `SeekToBeginning/End/Time` (ListOffsets); duration-based reset (`WithConsumeSince`, KIP-1106); OffsetFetch **v7 `require_stable`** (KIP-447) for EOS correctness.
- **read_committed** — filters aborted-transaction records on the Fetch path.
- Static membership (`group.instance.id`, KIP-345); leader-epoch fencing (KIP-320).

## Related
- [[features/consumer-lag]] · [[features/exactly-once-tv2]] · [[features/next-gen-groups]] · [[features/share-groups]]
