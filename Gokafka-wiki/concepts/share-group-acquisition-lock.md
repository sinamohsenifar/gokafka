---
title: Share-group acquisition lock & delivery count
type: concept
category: Concepts
subcategory: KIP-932
status: reference
tags: [gokafka, concept, kip-932]
updated: 2026-06-30
---

# Share-group acquisition lock & delivery count

How [[Research: KIP-932 share groups (Queues for Kafka)|KIP-932]] gives queue semantics per *record* instead of per *partition* (Source: [[sources/apache-kip-932]]).

When a consumer `ShareFetch`es, the share-partition leader **acquires** each record under a time-limited lock (default **30s**, `share.record.lock.duration.ms`). Four states:

| State | Meaning |
|---|---|
| **Available** | eligible to be acquired |
| **Acquired** | locked to one consumer (the lock window) |
| **Acknowledged** | processed — never redelivered |
| **Archived** | terminal — not eligible (rejected, or past SPSO) |

Actions on an acquired record:
- **Accept** → Acknowledged.
- **Release** → back to Available (retry transient errors).
- **Reject** → Archived (permanent / unprocessable).
- **Renew** → extend the lock (long-running work; ShareAcknowledge v2 / KIP-1222).
- **Lock timeout** (no action) → back to Available, **unless** the per-record delivery count has reached `group.share.delivery.attempt.limit` (default **5**) → then Archived (kills poison-message loops).

This yields **at-least-once with redelivery and out-of-order delivery** — the trade for letting more consumers than partitions share the work.

**In GoKafka:** `ShareConsumer.Acknowledge` maps to Accept/Release/Reject; Renew via ShareAcknowledge v2. See [[features/share-groups]].

## Related
[[concepts/share-coordinator-state]] · [[sources/apache-kip-932]] · [[features/share-groups]] · [[Research: KIP-932 share groups (Queues for Kafka)]] · [[Audit: KIP-932 implementation gaps]] · [[packages/consumer]]
