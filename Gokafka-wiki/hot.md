---
title: Hot cache
type: hot
category: Home
subcategory: Cache
status: active
tags: [gokafka, hot]
updated: 2026-06-30
---

# 🔥 Hot cache

Quick-reference of what's most active in the vault. Newest at top.

## Latest audit
**[[Audit: Protobuf & Schema Registry completeness]]** (2026-06-30, code-verified)
- **Protobuf wire format + registry integration = complete & interoperable** (zigzag message-index framing tested vs Confluent golden bytes). Avro 57.9 ns/op, JSON 203 ns/op (measured).
- **Boundary (by design):** no Protobuf *message* codec — `EncodeProtobuf` wraps pre-encoded bytes (BYO `google.golang.org/protobuf`); a real codec is third-party → violates [[decisions/adr-stdlib-only|stdlib-only]].
- **Real gaps (stdlib-fixable):** schema **references** (multi-file `.proto` imports) not sent; **no e2e Protobuf integration test**; Protobuf decode lacks the schema-id guards Avro has. → being fixed.

## Latest research
**[[Research: KIP-932 share groups (Queues for Kafka)]]** (2026-06-30)
- Queue semantics: many consumers per partition; **per-record** 30s acquisition lock with **Accept/Release/Reject/Renew** + delivery-count limit (default 5) → at-least-once with redelivery.
- Client RPCs ShareGroupHeartbeat/ShareFetch/ShareAcknowledge; server state in `__share_group_state` via a Share Coordinator. **GA Kafka 4.2.**
- **GoKafka** ships the client side (APIs 76–79, Renew via ShareAck v2). Concepts: [[concepts/share-group-acquisition-lock]] · [[concepts/share-coordinator-state]]

**[[Research: KIP-848 next-gen consumer rebalance protocol]]** (2026-06-30)
- Server-driven, **incremental** rebalance replacing JoinGroup/SyncGroup; one `ConsumerGroupHeartbeat` RPC; **GA in Kafka 4.0**, opt-in `group.protocol=consumer`.
- Convergence via three epochs (group/assignment/member); assignment is **server-side by default** (`uniform`/`range`); client-side assignors not yet implemented.
- **GoKafka** already ships the client side (API 68/69, RE2J regex, server-side assignment); [[compatibility/redpanda|Redpanda]] doesn't support it yet.
- Concepts: [[concepts/consumergroupheartbeat]] · [[concepts/epoch-reconciliation]] · [[concepts/server-side-assignor]]

## Entry points
- [[index]] — Map of Content (by category) · [[Dashboard]] — 📊 analytics & status · [[meta/taxonomy|Taxonomy]] · [[repo-docs]] — repo↔wiki bridge

## Open questions (from research)
- Quantified rebalance-time gains (no authoritative numbers found).
- Client-side assignor + rack-aware timelines (KAFKA-18327 / KAFKA-17747).
- Redpanda ConsumerGroupHeartbeat support ETA.
