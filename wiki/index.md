---
title: GoKafka Wiki — Map of Content
type: moc
tags: [gokafka, moc, index]
updated: 2026-06-30
---

# GoKafka Wiki

A knowledge map of **GoKafka** — a pure-Go (stdlib-only, no CGO) Apache Kafka client. This vault documents the architecture, wire-protocol implementation, feature set, design decisions, competitor analysis, and the development history.

> [!info] What is GoKafka?
> A from-scratch Kafka client in pure Go: producer, consumer groups (classic + KIP-848), transactions/EOS (KIP-890 TV2), share groups (KIP-932), admin (43 API keys), Schema Registry + CSFLE, and an in-process mock broker. Targets Kafka 3.4–4.3 and Redpanda. **No third-party `go.mod` dependencies.**

> [!tip] Two ways in
> - **[[Dashboard|📊 Dashboard]]** — live data-driven tables (Bases) over every note.
> - **This page** — the curated, hand-linked Map of Content. Open **graph view** (⌘/Ctrl-G) to see it all connected, or **[[meta/architecture-map.canvas|the architecture canvas]]**.

## Architecture
- [[architecture/overview|Architecture overview]]
- [[architecture/client-lifecycle|Client lifecycle & connect]]
- [[architecture/cluster-coordinator|Cluster: metadata, leaders, coordinators]]
- [[architecture/transport|Transport: framing & connections]]
- [[architecture/wire-protocol|Wire protocol: flexible vs legacy]]

## Packages
- [[packages/producer|Producer]] · [[packages/partitioners|Partitioners]] · [[packages/compression|Compression]]
- [[packages/consumer|Consumer & groups]] · [[features/consumer-lag|Consumer lag]]
- [[packages/transactions|Transactions / EOS]]
- [[packages/admin|Admin]]
- [[packages/schema-registry|Schema Registry & serdes]]
- [[packages/kfake-mock-broker|kfake — in-process mock broker]]
- [[packages/observability|Observability]]

## Protocol
- [[protocol/api-coverage|API coverage (43 keys)]]
- [[protocol/kip-coverage|KIP coverage]]
- [[protocol/flexible-encoding|Flexible encoding & the decode-bug pattern]]
- [[protocol/version-negotiation|Version negotiation]]

## Features
- [[features/exactly-once-tv2|Exactly-once / KIP-890 TV2]]
- [[features/share-groups|Share groups (KIP-932)]]
- [[features/next-gen-groups|Next-gen consumer groups (KIP-848)]]
- [[features/csfle|Client-side field-level encryption (CSFLE)]]

## Compatibility
- [[compatibility/kafka-versions|Kafka 3.9–4.3]]
- [[compatibility/redpanda|Redpanda]]
- [[compatibility/broker-quirks|Broker quirks & decode bugs]]

## Decisions (ADRs)
- [[decisions/adr-stdlib-only|Stdlib-only, no CGO]]
- [[decisions/adr-mock-as-oracle|Real client as the mock-broker oracle]]
- [[decisions/adr-tv2-incremental|Incremental KIP-890 TV2 landing]]

## Competitors
- [[competitors/parity-matrix|Parity matrix]]
- [[competitors/franz-go|franz-go]] · [[competitors/sarama|IBM/sarama]] · [[competitors/kafka-go|segmentio/kafka-go]] · [[competitors/confluent-kafka-go|confluent-kafka-go]]

## History
- [[history/releases|Release history (v0.25 → v0.26.7)]]
- [[history/session-2026-06-30|Session log: competitor parity + Redpanda]]

## Meta & tooling
- [[Dashboard]] — live Bases views · [[meta/architecture-map.canvas|Architecture canvas]]
- [[repo-docs|Repo docs ↔ wiki map]] — bridge to the canonical `docs/`
- [[meta/conventions|Vault conventions]] — frontmatter, folders, linking
- Templates: `meta/templates/` (`page`, `adr`) · Bases: `meta/*.base`

---
- 🪵 [[log|Activity log]]
