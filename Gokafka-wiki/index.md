---
title: GoKafka Wiki — Map of Content
type: moc
category: Home
subcategory: Map
status: active
tags: [gokafka, moc, index]
updated: 2026-06-30
---

# GoKafka Wiki

A knowledge map of **GoKafka** — a pure-Go (stdlib-only, no CGO) Apache Kafka client. This vault documents the architecture, wire-protocol implementation, feature set, design decisions, competitor analysis, and development history, organized by the [[meta/taxonomy|taxonomy]] (category → sub-category).

> [!info] What is GoKafka?
> A from-scratch Kafka client in pure Go: producer, consumer groups (classic + KIP-848), transactions/EOS (KIP-890 TV2), share groups (KIP-932), admin (47 API keys), Schema Registry + CSFLE, and an in-process mock broker. Targets Kafka 3.4–4.3 and Redpanda. **No third-party `go.mod` dependencies.**

> [!tip] Three ways in
> - **[[Dashboard|📊 Analytics & Status]]** — release cadence, capability status, coverage charts, live Bases.
> - **This page** — the curated, hand-linked Map of Content, by category.
> - **Graph** — open graph view (⌘/Ctrl-G), the **[[meta/architecture-map.canvas|architecture canvas]]**, or the 3D Graph / Extended Graph plugins (color by `category` / `type` / tags).

## 🏛️ Architecture
- [[architecture/overview|Overview]] · [[architecture/client-lifecycle|Client lifecycle]] · [[architecture/cluster-coordinator|Cluster: metadata & coordinators]]
- [[architecture/transport|Transport]] · [[architecture/wire-protocol|Wire protocol]]

## 🔌 Protocol
- [[protocol/api-coverage|API coverage (47 keys)]] · [[protocol/kip-coverage|KIP coverage]]
- [[protocol/flexible-encoding|Flexible encoding & the decode-bug pattern]] · [[protocol/version-negotiation|Version negotiation]]

## 📦 Packages
- **Producer:** [[packages/producer|Producer]] · [[packages/partitioners|Partitioners]] · [[packages/compression|Compression]]
- **Consumer:** [[packages/consumer|Consumer & groups]]
- **EOS:** [[packages/transactions|Transactions / EOS]]
- **Admin:** [[packages/admin|Admin]]
- **Schema:** [[packages/schema-registry|Schema Registry & serdes]]
- **Testing:** [[packages/kfake-mock-broker|kfake mock broker]]
- **Observability:** [[packages/observability|Observability]]

## ✨ Features
- **Groups:** [[features/share-groups|Share groups (KIP-932)]] · [[features/next-gen-groups|Next-gen groups (KIP-848)]]
- **EOS:** [[features/exactly-once-tv2|Exactly-once / KIP-890 TV2]]
- **Security:** [[features/csfle|CSFLE]]
- **Observability:** [[features/consumer-lag|Consumer lag]]

## 🧩 Concepts
- **KIP-848:** [[concepts/consumergroupheartbeat|ConsumerGroupHeartbeat]] · [[concepts/epoch-reconciliation|Epoch reconciliation]] · [[concepts/server-side-assignor|Server-side assignment]]
- **KIP-932:** [[concepts/share-group-acquisition-lock|Acquisition lock]] · [[concepts/share-coordinator-state|Share coordinator state]]

## 🔬 Research & audits
- **Syntheses:** [[Research: KIP-848 next-gen consumer rebalance protocol|KIP-848 deep dive]] · [[Research: KIP-932 share groups (Queues for Kafka)|KIP-932 deep dive]]
- **Code-verified audits:** [[Audit: KIP-932 implementation gaps|KIP-932 gaps]] (HIGH gaps fixed v0.26.8–10) · [[Audit: Protobuf & Schema Registry completeness|Protobuf & SR]] (fixed v0.26.11)
- **Sources:** [[sources/apache-kip-848|Apache KIP-848]] · [[sources/kafka-docs-rebalance-protocol|Kafka docs]] · [[sources/confluent-kip-848-blog|Confluent KIP-848]] · [[sources/apache-kip-932|Apache KIP-932]] · [[sources/confluent-share-consumer-ga|Confluent share GA]]

## 🔁 Compatibility
- [[compatibility/kafka-versions|Kafka 3.9–4.3]] · [[compatibility/redpanda|Redpanda]] · [[compatibility/broker-quirks|Broker quirks & decode bugs]]

## ⚖️ Decisions (ADRs)
- [[decisions/adr-stdlib-only|Stdlib-only, no CGO]] · [[decisions/adr-mock-as-oracle|Real client as mock-broker oracle]] · [[decisions/adr-tv2-incremental|Incremental KIP-890 TV2]]

## 🥇 Competitors
- [[competitors/parity-matrix|Parity matrix]]
- [[competitors/franz-go|franz-go]] · [[competitors/sarama|IBM/sarama]] · [[competitors/kafka-go|segmentio/kafka-go]] · [[competitors/confluent-kafka-go|confluent-kafka-go]]

## 🕓 History
- [[history/releases|Release history (v0.24 → v0.26.11)]] · 🪵 [[log|Activity log]]

## 🗂️ Meta & tooling
- [[Dashboard|Analytics dashboard]] · [[meta/architecture-map.canvas|Architecture canvas]]
- [[meta/taxonomy|Taxonomy & schema]] · [[meta/conventions|Vault conventions]]
- [[repo-docs|Repo docs ↔ wiki map]] — bridge to the canonical `docs/`
- Templates: `meta/templates/` · Bases: `meta/*.base`
