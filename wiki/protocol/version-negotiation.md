---
title: Version negotiation
type: protocol
tags: [gokafka, protocol, negotiation]
updated: 2026-06-30
---

# Version negotiation

At connect, `Cluster.NegotiateVersions` sends **ApiVersions v3** and, for each API the broker advertises, stores `min(clientCeiling, brokerMax)`. Calls then use the negotiated version, so the client adapts to any broker (Kafka 3.9–4.3, [[compatibility/redpanda|Redpanda]]).

## Subtleties (hard-won)
- **v0-max APIs must be stored.** An API a broker advertises with max=0 negotiates to v0 — that must override the client's higher pinned default, or the broker resets the connection. `ClientVersion` returns **-1** for unimplemented APIs to distinguish "v0 supported" from "unknown". ([[compatibility/broker-quirks|bug #2]])
- **`AdvertisesAPI`** — admin returns a clear error for APIs the broker never advertised, instead of an opaque EOF.
- Some APIs are **pinned** (e.g. OffsetFetch single v7 for `require_stable`); most are negotiated down.

## Related
- [[compatibility/broker-quirks]] · [[protocol/api-coverage]] · [[architecture/client-lifecycle]]
