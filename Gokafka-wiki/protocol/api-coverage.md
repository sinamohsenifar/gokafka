---
title: API coverage (43 keys)
type: protocol
category: Protocol
subcategory: Coverage
status: stable
tags: [gokafka, protocol, api]
updated: 2026-06-30
---

# API coverage (43 keys)

GoKafka implements **43 client-facing API keys**, version-negotiated with the broker. Authoritative list: `docs/CONFORMANCE.md`.

Highlights of notable max versions: Produce 12 (TV2), Fetch 13 (topic-ids, KIP-516), Metadata 12, ApiVersions 3, OffsetFetch 6/8 (single + multi-group), FindCoordinator 3, plus the KIP-848 (68/69), KIP-932 (76–79) next-gen/share APIs and the full admin surface incl. AlterPartitionReassignments (45/46), DescribeUserScramCredentials (50).

**Not implemented** (by design / niche): OffsetForLeaderEpoch (23 — truncation detection, fencing done), GetTelemetrySubscriptions/PushTelemetry (71/72 — KIP-714 non-goal), DescribeTopicPartitions (75), delegation tokens (38–41).

## Related
- [[protocol/kip-coverage]] · [[protocol/version-negotiation]] · [[packages/admin]]
- [[protocol/flexible-encoding]] · [[architecture/wire-protocol]] · [[compatibility/kafka-versions]] · [[compatibility/broker-quirks]]
