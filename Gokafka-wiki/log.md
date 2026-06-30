---
title: Activity Log
type: log
category: History
subcategory: Activity
status: active
tags: [gokafka, log]
updated: 2026-06-30
---

# Activity Log

Append-only log of wiki ingests and notable project events. Newest first.

## 2026-06-30
- **Knowledge-base refactor** — recategorized the whole vault to a unified taxonomy (`type`/`category`/`subcategory`/`status`/normalized `tags`; see [[meta/taxonomy]]). Enriched every note's `## Related` (graph connectivity), normalized all wikilinks to path form, deduped tags, removed the redundant session note. Rebuilt [[Dashboard]] as an analytics home (Charts View + Contribution Graph over the 40-release history), added the [[meta/status.base|status board]], and tuned the vault for the 3D Graph / Extended Graph / Charts View / Contribution Graph / Smart Connections plugins. Verified: 0 dead links, 0 frontmatter gaps, 0 duplicate tags.
- **Audit | Protobuf & Schema Registry completeness** — code-verified (read serde.go/registry.go/confluent.go, ran unit tests + serde benchmarks, grepped integration coverage). Created [[Audit: Protobuf & Schema Registry completeness]]. Key finding: Confluent Protobuf **wire framing + registry integration are complete and interoperable** (message-index zigzag framing unit-tested vs Confluent golden bytes; Avro 57.9 ns/op, JSON 203 ns/op measured), but there is **no Protobuf message codec** (BYO pre-encoded bytes — deliberate [[decisions/adr-stdlib-only|stdlib-only]] boundary). Real stdlib-fixable gaps: no schema **references** (multi-file `.proto`), **no end-to-end Protobuf integration test**, Protobuf decode missing the schema-id guards Avro has. → fixing.
- **v0.26.9–v0.26.10** shipped — KIP-932 share-group ack modes (implicit/explicit) + share-consumer connection-robustness fixes (PR #43); clear unsupported-broker error (PR #44). Closed audit HIGH #2/#3/#4 + the MEDIUM unsupported-broker guard. See [[Audit: KIP-932 implementation gaps]].
- **autoresearch | KIP-932 share groups (Queues for Kafka)** — rounds: 1 · sources: 2 (Apache cwiki spec, Confluent GA blog) · pages: [[Research: KIP-932 share groups (Queues for Kafka)]] (synthesis), [[sources/apache-kip-932]], [[sources/confluent-share-consumer-ga]], [[concepts/share-group-acquisition-lock]], [[concepts/share-coordinator-state]] · key finding: queue semantics via per-record acquisition locks (30s, Accept/Release/Reject/Renew) + delivery-count limit; many consumers per partition; state in `__share_group_state` via a Share Coordinator; **GA in Kafka 4.2**. GoKafka implements the client side (APIs 76–79).
- **autoresearch | KIP-848 next-gen consumer rebalance protocol** — rounds: 2 · sources: 3 (Apache cwiki spec, Kafka 4.1 docs, Confluent blog) · pages created: [[Research: KIP-848 next-gen consumer rebalance protocol]] (synthesis), [[sources/apache-kip-848]], [[sources/kafka-docs-rebalance-protocol]], [[sources/confluent-kip-848-blog]], [[concepts/consumergroupheartbeat]], [[concepts/epoch-reconciliation]], [[concepts/server-side-assignor]] · key finding: KIP-848 replaces client-driven JoinGroup/SyncGroup with a server-driven, incremental `ConsumerGroupHeartbeat` protocol (GA in Kafka 4.0, opt-in `group.protocol=consumer`); GoKafka already implements its client side with server-side assignment.
- **Docs management** — created `docs/README.md` (index), `docs/ARCHITECTURE.md`, `docs/REDPANDA.md`; updated the README docs table; added [[repo-docs|repo↔wiki map]] bridging the vault to the canonical `docs/`. Installed **rtk** (Rust Token Killer) and routed all CLI commands through it.
- **Obsidian power-up** — added Bases (`meta/dashboard.base`, `decisions.base`, `competitors.base`), a [[Dashboard]] note, the [[meta/architecture-map.canvas|architecture canvas]], templates (`meta/templates/`), [[meta/conventions|conventions]], and enabled core plugins (bases, graph, backlinks, properties).
- **Wiki bootstrapped** — scaffolded the GoKafka codebase-mapping vault (architecture, packages, protocol, features, compatibility, decisions, competitors, history).
- **v0.26.7** shipped — Redpanda CI lane + CreatePartitions decode fix + portable integration suite. See [[compatibility/redpanda]].
- **v0.26.6** shipped — Redpanda protocol compatibility: DescribeConfigs synonym decode bug, v0-API version negotiation, clear unsupported-API errors. See [[compatibility/broker-quirks]].
- **v0.26.0–v0.26.3** — kfake mock broker, CSFLE, SR strategies, partition reassignments, header framing. See [[history/releases]].
- **v0.25.21–v0.25.26** — competitor-parity pass (partitioners, lag, mock registry, examples, TLS hint, require_stable, DescribeUserScramCredentials). See [[competitors/parity-matrix]].
