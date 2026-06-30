---
title: Dashboard
type: dashboard
category: Home
subcategory: Analytics
status: active
tags: [gokafka, dashboard, analytics]
updated: 2026-06-30
---

# 📊 GoKafka — Analytics & Status

The analytical home for the vault: release cadence, capability status, coverage, and live Bases over every note. For the curated, hand-linked map see [[index|the Map of Content]].

> [!tip] Navigation
> - **[[index]]** — Map of Content (categories & sub-categories)
> - **Graph view** (⌘/Ctrl-G) or the **[[meta/architecture-map.canvas|architecture canvas]]** — the link graph (3D Graph / Extended Graph plugins color by `category`, `type`, and tags)
> - **[[meta/taxonomy|Taxonomy]]** — how every note is categorized
> - **[[repo-docs|Repo docs ↔ wiki]]** — bridge to the canonical `docs/`

## 🗓️ Release activity
40 tagged releases: 1 on Jun 24, 27 on Jun 29, 12 on Jun 30 (source: `git tag` / `CHANGELOG.md`).

```contributionGraph
title: 'GoKafka releases'
graphType: default
dateRangeType: FIXED_DATE_RANGE
fromDate: 2026-06-22
toDate: 2026-06-30
startOfWeek: 0
data:
  - date: '2026-06-24'
    value: 1
  - date: '2026-06-29'
    value: 27
  - date: '2026-06-30'
    value: 12
```

```chartsview
type: Column
data:
  - { day: "Jun 24 (v0.24)", releases: 1 }
  - { day: "Jun 29 (v0.25.x)", releases: 27 }
  - { day: "Jun 30 (v0.26.x)", releases: 12 }
options:
  xField: day
  yField: releases
  label: { position: middle }
  color: "#7c3aed"
```

## 📦 Capability & version status
Authoritative facts live in `docs/CONFORMANCE.md` / `CHANGELOG.md`; this is the at-a-glance view.

| Capability | Status | Since | Notes |
|---|---|---|---|
| Idempotent + transactional producer | ✅ GA | 0.24 | acks, batching, compression |
| Consumer groups (classic) | ✅ GA | 0.24 | range/roundrobin/sticky/coop-sticky |
| [[features/next-gen-groups\|Next-gen groups (KIP-848)]] | ✅ GA | 0.25.9 | server-side assignment, RE2J regex |
| [[features/exactly-once-tv2\|Exactly-once / KIP-890 TV2]] | ✅ GA | 0.25.14 | Produce v12, EndTxn v5 |
| [[features/share-groups\|Share groups (KIP-932)]] | ✅ GA | 0.25.19 | client side, Renew, implicit/explicit ack (0.26.9) |
| [[packages/admin\|Admin]] (47 API keys) | ✅ GA | 0.26.5 | quotas, SCRAM, delegation tokens, reassignment |
| [[packages/schema-registry\|Schema Registry + serdes]] | ✅ GA | 0.25.x | Avro/JSON in-lib, Protobuf framing + references (0.26.11) |
| [[features/csfle\|CSFLE]] | ✅ GA | 0.26.1 | pure-Go envelope encryption |
| [[packages/kfake-mock-broker\|kfake mock broker]] | ✅ GA | 0.26.0 | in-process, real client as oracle |
| [[compatibility/redpanda\|Redpanda]] | ✅ CI-verified | 0.26.7 | dedicated CI lane |
| KIP-714 client metrics | ⛔ Non-goal | — | needs OTLP protobuf → breaks [[decisions/adr-stdlib-only\|stdlib-only]] |

```chartsview
type: Pie
data:
  - { state: "GA / shipped", value: 10 }
  - { state: "Stable", value: 1 }
  - { state: "Documented non-goal", value: 1 }
options:
  angleField: value
  colorField: state
  radius: 0.85
  label: { type: outer }
```

## 🧭 Coverage
```chartsview
type: Radar
data:
  - { dimension: "Producer / EOS", coverage: 100 }
  - { dimension: "Consumer groups", coverage: 100 }
  - { dimension: "Share groups", coverage: 95 }
  - { dimension: "Admin APIs", coverage: 95 }
  - { dimension: "Schema / serde", coverage: 90 }
  - { dimension: "Security", coverage: 90 }
  - { dimension: "Observability", coverage: 85 }
  - { dimension: "Mock / testing", coverage: 100 }
options:
  xField: dimension
  yField: coverage
  meta: { coverage: { min: 0, max: 100 } }
  area: {}
  point: {}
```
Coverage is GoKafka's self-assessed completeness per area (see [[competitors/parity-matrix]] for the head-to-head against franz-go / sarama / kafka-go / confluent-kafka-go).

## 🗂️ Live views (Bases)
Status board — every note by lifecycle status:
![[status.base]]

All pages by category:
![[dashboard.base]]

Architecture decisions:
![[decisions.base]]

Competitor clients:
![[competitors.base]]

## 📈 Quick stats
- **~58 notes** across 11 categories (see [[meta/taxonomy|the taxonomy]]).
- **40 releases** v0.24.1 → v0.26.11; every change shipped via branch → PR → full CI matrix (Kafka 3.9.2–4.3.0 × Go 1.22–1.26 + Redpanda) → merge → tag.
- **47 client API keys**, **5 KIPs at GA** (848 / 890 / 932 / 455 / 554) — see [[protocol/kip-coverage]] · [[protocol/api-coverage]].
- Source of truth: repo `README.md`, `CHANGELOG.md`, `docs/CONFORMANCE.md`.

## Related
- [[index]] · [[meta/taxonomy]] · [[history/releases]] · [[competitors/parity-matrix]] · [[protocol/kip-coverage]] · [[repo-docs]]
