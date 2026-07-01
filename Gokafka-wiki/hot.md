---
title: Hot cache
type: hot
category: Home
subcategory: Cache
status: active
tags: [gokafka, hot]
updated: 2026-07-01
---

# 🔥 Hot cache

Quick-reference of what's most active in the vault. Newest at top.

## Latest audit — data-loss / message-integrity (2026-07-01, v0.26.22–v0.26.27)
Adversarial find→verify Workflow over produce/consume/rebalance/share flows: 16 candidates → **6 confirmed integrity bugs, all fixed** (one PR+release each, regression test each, real-broker verified; a second design+verify Workflow adversarially reviewed the fixes and caught draft regressions). The 6: (1) record-batch **CRC-32C validation** on consume; (2) idempotent **sequence per-record** not per-batch + **OUT_OF_ORDER/epoch made fatal** (were retriable + reset-PID-and-resend → dup); (3) **OffsetFetch completeness** — never resume an assigned partition at offset 0 on a transient/omitted response (mass re-read); (4) **freeze partition across produce retries** (RoundRobin re-partition on retry → reorder/dup); (5) **Poll delivered-position cursor** — multi-broker parallel fetch + `out[:maxPoll]` truncation was silently dropping already-cursor-bumped records; (6) **per-partition produce retry** — partial multi-broker failure re-sent the whole batch → dup of committed partitions (idempotence off), validated on a real 3-broker cluster. New `kfake` fault-injection knobs. See [[history/releases]].

## Earlier audit
**[[Audit: Protobuf & Schema Registry completeness]]** (2026-06-30, code-verified)
- **Protobuf wire format + registry integration = complete & interoperable** (zigzag message-index framing tested vs Confluent golden bytes). Avro 57.9 ns/op, JSON 203 ns/op (measured).
- **Boundary (by design):** no Protobuf *message* codec — `EncodeProtobuf` wraps pre-encoded bytes (BYO `google.golang.org/protobuf`); a real codec is third-party → violates [[decisions/adr-stdlib-only|stdlib-only]].
- **Real gaps (stdlib-fixable):** schema **references** (multi-file `.proto` imports) not sent; **no e2e Protobuf integration test**; Protobuf decode lacks the schema-id guards Avro has. → being fixed.

## Latest research — frontier (2026-06-30)
- **[[Research: KIP-932 share-group configuration & remaining client surface]]** — `share.*` group configs altered via IncrementalAlterConfigs on the GROUP resource (type 32). GoKafka gaps: no alter-group-config admin API (only hardcoded `share.auto.offset.reset=earliest`), `delivery_count` decoded-then-dropped (`internal/protocol/share_fetch.go:197`), default ack mode explicit vs Java's implicit.
- **[[Research: KIP-848 client-side assignors & rack-aware assignment]]** — client-side assignors still unimplemented upstream (KAFKA-18327 Open); rack-aware is broker-side; both **non-goals** for GoKafka's thin client.
- **[[Research: Redpanda next-gen group & share-group roadmap]]** — Redpanda supports neither KIP-848 nor KIP-932; no announced ETA (issue #29223). GoKafka's auto-skip is correct.
- **[[Research: Apache Kafka 4.x roadmap & upcoming KIPs]]** — all client-facing 4.0–4.2 features already shipped; KIP-1150 diskless / KIP-966 ELR are broker-internal non-goals; client-side candidates: KIP-1102 rebootstrap, KIP-1191 DLQ. KIP-1274 (4.3) deprecates classic JoinGroup → validates the KIP-848 investment.

Foundational deep-dives: [[Research: KIP-932 share groups (Queues for Kafka)]] · [[Research: KIP-848 next-gen consumer rebalance protocol]]

## Entry points
- [[index]] — Map of Content (by category) · [[Dashboard]] — 📊 analytics & status · [[meta/taxonomy|Taxonomy]] · [[repo-docs]] — repo↔wiki bridge

## Open questions (from research)
- ~~Quantified rebalance-time gains~~ → ~20x in one disclosed benchmark (100→1000 partitions, 10 consumers): 103s → 5s. See [[Research: KIP-848 client-side assignors & rack-aware assignment]].
- ~~Client-side assignor + rack-aware timelines~~ → client-side assignors still unimplemented (KAFKA-18327, Open); rack-aware *trigger* (KIP-1101/KAFKA-17747) back in Kafka 4.1; both non-goals for GoKafka.
- Does GoKafka's heartbeat send a `client.rack` id for future broker rack-aware assignment? (needs code check)
- ~~Redpanda ConsumerGroupHeartbeat / share-group ETA~~ → none announced; Redpanda supports neither (issue #29223). See [[Research: Redpanda next-gen group & share-group roadmap]].
- New: does GoKafka's rebootstrap behaviour match KIP-1102? (needs code check) · should `delivery_count` be surfaced on `Record`? (KIP-932 gap)
