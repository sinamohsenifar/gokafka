---
title: "Research: KIP-932 share groups (Queues for Kafka)"
type: research
category: Research
subcategory: Deep dive
status: complete
tags: [gokafka, research, kip-932]
updated: 2026-06-30
related:
  - "[[features/share-groups]]"
  - "[[concepts/share-group-acquisition-lock]]"
  - "[[concepts/share-coordinator-state]]"
sources:
  - "[[sources/apache-kip-932]]"
  - "[[sources/confluent-share-consumer-ga]]"
---

# Research: KIP-932 — share groups (Queues for Kafka)

## Overview
KIP-932 adds **share groups**: a queue-style consumption model where **many consumers cooperatively read the same partitions** with **per-record** acknowledgement and delivery counting — decoupling consumer count from partition count. **GA in Apache Kafka 4.2** (early access 3.x/4.0 → preview → GA), gated by the `share.version` feature.

## Key findings
- **Per-record, not per-partition.** A share group can have more consumers than partitions; records are acquired under a **30s lock** and can be redelivered → **at-least-once, possibly out of order** (Source: [[sources/apache-kip-932]]). See [[concepts/share-group-acquisition-lock]].
- **Three client RPCs:** `ShareGroupHeartbeat` (session), `ShareFetch` (fetch + piggyback ack, stateful share sessions), `ShareAcknowledge` (explicit ack) (Source: [[sources/apache-kip-932]]).
- **Lock state machine:** Available → Acquired → Acknowledged / Archived, with actions **Accept / Release / Reject / Renew**; `group.share.delivery.attempt.limit` (default **5**) archives poison messages (Sources: both). See [[concepts/share-group-acquisition-lock]].
- **Acknowledgement modes:** *implicit* (default; `poll()` auto-accepts) vs *explicit* (`acknowledge(record, AcknowledgeType.X)`) (Source: [[sources/confluent-share-consumer-ga]]).
- **New server-side state:** a distributed **Share Coordinator** persists state to the internal **`__share_group_state`** topic (ShareSnapshot + ShareUpdate), bounded by SPSO…SPEO (Source: [[sources/apache-kip-932]]). See [[concepts/share-coordinator-state]].
- **Use cases:** work queues, job processing, command/event workflows with back-pressure (Source: [[sources/confluent-share-consumer-ga]]).

## How this maps to GoKafka
GoKafka's **`ShareConsumer`** implements the client side end-to-end: `ShareGroupHeartbeat` (76), `ShareGroupDescribe` (77), `ShareFetch` (78), `ShareAcknowledge` (79) — with **Accept/Release/Reject** and **Renew** (ShareAcknowledge v2 = KIP-1222). It needs none of the inter-broker share-coordinator RPCs (server-side only). Requires a broker with `share.version=1` (Kafka 4.1+); [[compatibility/redpanda|Redpanda]] doesn't implement it, so GoKafka detects and skips. See [[features/share-groups]].

> Among the Go clients, only GoKafka and franz-go implement share groups ([[competitors/parity-matrix]]).

## Key concepts
- [[concepts/share-group-acquisition-lock]]: the 30s lock + 4 states + delivery limit.
- [[concepts/share-coordinator-state]]: ShareCoordinator + `__share_group_state` + SPSO/SPEO.

## Contradictions
- None. The Confluent GA blog matches the primary spec; it only adds the implicit/explicit ack-mode framing and the GA version (Kafka 4.2), which the spec excerpt didn't pin.

## Open questions
- **Throughput numbers:** vendor "elastic scaling" claims are unquantified — needs measurement.
- **DLQ:** KIP-1191 (dead-letter queues for share groups) — status/availability?
- **Exactly-once for share groups:** current semantics are at-least-once; is EOS on the roadmap?
- **Redpanda support:** when will Redpanda implement the share-group APIs (76–79)?
- **GoKafka coverage of `share.acknowledgement.mode`:** does GoKafka expose implicit vs explicit, and `share.auto.offset.reset`? (verify against `share_consumer.go`)

## Sources
- [[sources/apache-kip-932]] — Apache cwiki, primary spec (high)
- [[sources/confluent-share-consumer-ga]] — Confluent GA blog (medium; perf claims qualitative)

## Related
[[features/share-groups|Share groups (KIP-932)]] · [[concepts/share-group-acquisition-lock|Share-group acquisition lock]] · [[concepts/share-coordinator-state|Share coordinator & __share_group_state]] · [[sources/apache-kip-932|Apache cwiki KIP-932]] · [[sources/confluent-share-consumer-ga|Confluent share consumer GA]] · [[competitors/parity-matrix|Competitor parity matrix]] · [[Audit: KIP-932 implementation gaps]] · [[protocol/api-coverage|API coverage]]
