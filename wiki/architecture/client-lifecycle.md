---
title: Client lifecycle & connect
type: architecture
tags: [gokafka, architecture]
updated: 2026-06-30
---

# Client lifecycle & connect

`gokafka.NewClient(cfg)` (root `client.go`):
1. Builds the [[cluster-coordinator|Cluster]] from `cfg.Brokers` + security.
2. `Cluster.NegotiateVersions` — sends **ApiVersions v3**, stores the negotiated version per API ([[protocol/version-negotiation]]) and any cluster finalized features ([[features/exactly-once-tv2|transaction.version]]).
3. `Cluster.Refresh(nil)` — fetches **Metadata**; on failure, `tlsMismatchHint` adds a hint if a plaintext client hit a TLS-only broker (an opaque EOF otherwise).

`Client.Close()` releases broker connections; idempotent.

## Related
- [[overview]] · [[transport]] · [[compatibility/redpanda]]
