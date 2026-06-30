---
title: Broker quirks & decode bugs
type: compatibility
tags: [gokafka, compatibility, bugs]
updated: 2026-06-30
---

# Broker quirks & decode bugs

Three real protocol bugs found by running the suite against [[compatibility/redpanda|Redpanda]]. All were **latent against Apache Kafka** and follow the [[protocol/flexible-encoding|decode-bug pattern]] (a missing field absorbed by the next tag-skip when the value is null).

## 1. DescribeConfigs v4 — missing per-synonym tag section (v0.26.6)
The flexible decoder read each config's synonyms as `{name, value, source}` but omitted the **trailing tag section** on each synonym struct. A freshly-created Kafka topic has no synonyms (loop never runs), so it stayed hidden. Redpanda returns synonyms for overridden topic configs → stream desync → "buffer too short". Also affects Kafka for any **non-default** config.
- Fix: add `buf.SkipTagSection()` per synonym in `decodeDescribeConfigsFlex` (`internal/protocol/admin.go`).

## 2. Version negotiation dropped v0-max APIs (v0.26.6)
`NegotiateVersions` had an `if ver > 0` guard, so an API a broker advertises with **max version 0** (e.g. `ListTransactions` on Redpanda) was dropped from the negotiated map. The client then fell back to its own higher pinned version and the broker **reset the connection** (opaque EOF).
- Fix: store the negotiated version even when 0; `ClientVersion` returns `-1` for unimplemented APIs to distinguish "v0 supported" from "unknown" (`internal/broker/cluster.go`, `internal/protocol/versions.go`).
- Bonus: new `Cluster.AdvertisesAPI` lets `Admin` return a clear error for genuinely unadvertised APIs.

## 3. CreatePartitions v2 — missing per-topic error_message (v0.26.7)
The flexible decoder read `{name, error_code}` then the tag section, skipping `error_message`. Worked on Kafka when the message is null (the `0x00` absorbed by the tag-skip); desynced against Redpanda's non-null message.
- Fix: read `error_message` (compact nullable string) in `decodeCreatePartitionsResponseFlex`. The legacy decoder was already correct.

## Other broker differences (not bugs)
- **Controller id 0** is valid on Redpanda (single node) — tests must not treat 0 as invalid.
- **Broker node ids** differ (Kafka stack often `1`, Redpanda `0`) — derive from `DescribeCluster`.
- Redpanda doesn't advertise KRaft **finalized features**.

## Related
- [[protocol/flexible-encoding]] · [[protocol/version-negotiation]] · [[compatibility/redpanda]]
