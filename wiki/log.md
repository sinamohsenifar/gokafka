---
title: Activity Log
type: log
tags: [gokafka, log]
updated: 2026-06-30
---

# Activity Log

Append-only log of wiki ingests and notable project events. Newest first.

## 2026-06-30
- **Docs management** — created `docs/README.md` (index), `docs/ARCHITECTURE.md`, `docs/REDPANDA.md`; updated the README docs table; added [[repo-docs|repo↔wiki map]] bridging the vault to the canonical `docs/`. Installed **rtk** (Rust Token Killer) and routed all CLI commands through it.
- **Obsidian power-up** — added Bases (`meta/dashboard.base`, `decisions.base`, `competitors.base`), a [[Dashboard]] note, the [[meta/architecture-map.canvas|architecture canvas]], templates (`meta/templates/`), [[meta/conventions|conventions]], and enabled core plugins (bases, graph, backlinks, properties).
- **Wiki bootstrapped** — scaffolded the GoKafka codebase-mapping vault (architecture, packages, protocol, features, compatibility, decisions, competitors, history).
- **v0.26.7** shipped — Redpanda CI lane + CreatePartitions decode fix + portable integration suite. See [[compatibility/redpanda]].
- **v0.26.6** shipped — Redpanda protocol compatibility: DescribeConfigs synonym decode bug, v0-API version negotiation, clear unsupported-API errors. See [[compatibility/broker-quirks]].
- **v0.26.0–v0.26.3** — kfake mock broker, CSFLE, SR strategies, partition reassignments, header framing. See [[history/releases]].
- **v0.25.21–v0.25.26** — competitor-parity pass (partitioners, lag, mock registry, examples, TLS hint, require_stable, DescribeUserScramCredentials). See [[competitors/parity-matrix]].
