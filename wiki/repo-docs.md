---
title: Repo docs ↔ wiki map
type: moc
tags: [gokafka, moc, docs]
updated: 2026-06-30
---

# Repo docs ↔ wiki map

The **canonical** documentation lives in the repository (rendered on GitHub / pkg.go.dev).
This vault is the **navigable knowledge layer** over it. This page maps the two so nothing
drifts.

> [!info] Where things live
> - **Canonical, versioned docs:** [`../docs/`](../docs/README.md) and top-level
>   `README.md` / `CHANGELOG.md`.
> - **Knowledge map (this vault):** synthesizes + cross-links them for graph/backlink
>   navigation. See [[index|Map of Content]] and the [[Dashboard]].

## Repo doc → matching wiki note(s)
| Repo doc | Wiki note(s) |
|---|---|
| [docs/README.md](../docs/README.md) (index) | [[index]] · [[Dashboard]] |
| [docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md) | [[architecture/overview]] · [[architecture/cluster-coordinator]] · [[architecture/transport]] · [[architecture/wire-protocol]] |
| [docs/CONFORMANCE.md](../docs/CONFORMANCE.md) | [[protocol/api-coverage]] · [[protocol/kip-coverage]] |
| [docs/KIPS.md](../docs/KIPS.md) | [[protocol/kip-coverage]] |
| [docs/REDPANDA.md](../docs/REDPANDA.md) | [[compatibility/redpanda]] · [[compatibility/broker-quirks]] |
| [docs/KAFKA_VERSIONS.md](../docs/KAFKA_VERSIONS.md) · [docs/COMPATIBILITY.md](../docs/COMPATIBILITY.md) | [[compatibility/kafka-versions]] |
| [docs/PERFORMANCE.md](../docs/PERFORMANCE.md) | [[packages/producer]] · [[packages/consumer]] |
| [docs/TESTING.md](../docs/TESTING.md) | [[packages/kfake-mock-broker]] · [[decisions/adr-mock-as-oracle]] |
| [docs/CAPABILITIES.md](../docs/CAPABILITIES.md) | [[index]] (feature pages) |
| [docs/GSSAPI.md](../docs/GSSAPI.md) · [docs/ZSTD.md](../docs/ZSTD.md) | [[packages/compression]] · security notes |

## Keeping them in sync
When a repo doc changes, update the matching wiki note's **Related** links and the
[[log|activity log]]. Authoritative facts (version numbers, API counts) come **from the
repo docs**; the wiki paraphrases and connects.

## Related
- [[index]] · [[Dashboard]] · [[meta/conventions]]
