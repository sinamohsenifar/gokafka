---
title: "Research: KIP-848 next-gen consumer rebalance protocol"
type: research
category: Research
subcategory: Deep dive
status: complete
tags: [gokafka, research, kip-848]
updated: 2026-06-30
related:
  - "[[features/next-gen-groups]]"
  - "[[concepts/consumergroupheartbeat]]"
  - "[[concepts/epoch-reconciliation]]"
  - "[[concepts/server-side-assignor]]"
sources:
  - "[[sources/apache-kip-848]]"
  - "[[sources/confluent-kip-848-blog]]"
  - "[[sources/kafka-docs-rebalance-protocol]]"
---

# Research: KIP-848 — the next-gen consumer rebalance protocol

## Overview
KIP-848 replaces Kafka's classic client-driven consumer rebalance (JoinGroup/SyncGroup) with a **server-driven, incremental** protocol built on a single `ConsumerGroupHeartbeat` RPC. The broker's group coordinator owns group state and (by default) computes assignments; consumers just declare subscriptions and acknowledge assign/revoke. It is **GA in Apache Kafka 4.0** (server-side assignors), opt-in via `group.protocol=consumer`.

## Key findings
- **The classic protocol's core flaw is a global synchronization barrier:** any join/leave/failure rebalances the whole group, so one bad consumer disrupts everyone; most fixes also required slow-to-adopt client changes (Source: [[sources/apache-kip-848]]).
- **One RPC replaces three.** `ConsumerGroupHeartbeat` subsumes JoinGroup + SyncGroup + Heartbeat; `ConsumerGroupDescribe` inspects state (Source: [[sources/apache-kip-848]]). See [[concepts/consumergroupheartbeat]].
- **Convergence is incremental via three epochs** (group / assignment / member); a partition is reassigned only after its prior owner revokes it — no stop-the-world (Source: [[sources/apache-kip-848]]). See [[concepts/epoch-reconciliation]].
- **Assignment is server-side by default** (`group.consumer.assignors` = `uniform`/`range`; client picks via `group.remote.assignor`); client-side assignors exist in the design for Kafka Streams but are **not yet implemented** (Source: [[sources/kafka-docs-rebalance-protocol]], KAFKA-18327). See [[concepts/server-side-assignor]].
- **Group state lives in `__consumer_offsets`** as compactable `ConsumerGroup*Metadata` records (X members → X+2 records) (Source: [[sources/apache-kip-848]]).
- **Config moved server-side:** `group.consumer.heartbeat.interval.ms` / `group.consumer.session.timeout.ms` replace the client `heartbeat.interval.ms` / `session.timeout.ms`; `partition.assignment.strategy` and `enforceRebalance()` are unavailable (Source: [[sources/kafka-docs-rebalance-protocol]]).
- **Upgrade is live and automatic:** a classic group converts to a consumer group when the first new-protocol member joins; online (rolling) and offline paths are documented; min **4.0**, recommended **4.2+** (Sources: [[sources/kafka-docs-rebalance-protocol]], [[sources/confluent-kip-848-blog]]).

## How this maps to GoKafka
GoKafka already implements the **client** side: `ConsumerGroupHeartbeat` (API 68) + `ConsumerGroupDescribe` (69), with **RE2J server-side regex** subscriptions (`Client.ConsumerPattern`). It uses **server-side assignment only**, so it needs no client-side assignor path — aligning with the protocol's "thin client" goal. See [[features/next-gen-groups]] and [[packages/consumer]]. Note: [[compatibility/redpanda|Redpanda]] (v26.1) does **not** yet implement ConsumerGroupHeartbeat, so GoKafka detects and skips it there.

## Key concepts
- [[concepts/consumergroupheartbeat]]: the unified heartbeat RPC.
- [[concepts/epoch-reconciliation]]: incremental convergence via group/assignment/member epochs.
- [[concepts/server-side-assignor]]: broker-computed assignment (uniform/range).

## Contradictions
- None substantive. Confluent's blog and the official docs agree with the primary spec on mechanics. The only divergence is **emphasis**: Confluent stresses performance ("faster rebalances") without numbers, which the spec/docs don't quantify.

## Open questions
- **Quantified performance:** no authoritative rebalance-time numbers were found (a 2024 "Current" conference talk exists but wasn't fetched). Treat magnitude claims as low confidence until measured.
- **Client-side assignor timeline:** unimplemented (KAFKA-18327) — when, and does it matter for non-Streams clients? (For GoKafka: no.)
- **Rack-aware assignment:** "not fully supported" (KAFKA-17747) — current state?
- **Deprecation runway:** `classic` is slated for deprecation/removal around Kafka 5.0 (KIP-1274); exact timeline unconfirmed.
- **Redpanda support:** when will Redpanda implement ConsumerGroupHeartbeat?

## Sources
- [[sources/apache-kip-848]] — Apache cwiki, primary spec (high)
- [[sources/kafka-docs-rebalance-protocol]] — Apache Kafka 4.1 official docs (high)
- [[sources/confluent-kip-848-blog]] — Confluent blog (medium; perf claims unverified)

## Related
[[features/next-gen-groups]] · [[concepts/consumergroupheartbeat]] · [[concepts/epoch-reconciliation]] · [[concepts/server-side-assignor]] · [[sources/apache-kip-848]] · [[sources/kafka-docs-rebalance-protocol]] · [[packages/consumer]] · [[protocol/kip-coverage]]
