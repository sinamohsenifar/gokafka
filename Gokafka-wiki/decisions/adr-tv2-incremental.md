---
title: "ADR: Incremental KIP-890 TV2 landing"
type: decision
category: Decisions
subcategory: ADR
status: accepted
tags: [gokafka, decision, eos]
updated: 2026-06-30
---

# ADR: Incremental KIP-890 TV2 landing

## Status
Accepted.

## Context
KIP-890 transactions v2 is EOS-correctness-critical and `transaction.version=2` is the **default on Kafka 4.x** — a wrong change would break transactions on all modern brokers.

## Decision
Land TV2 in safe increments, each independently shippable and CI-verified:
1. **v0.25.13** — capture cluster finalized features from ApiVersions v3 (zero EOS risk, read-only).
2. **v0.25.14** — gated TV2 produce path (skip `AddPartitionsToTxn`), determined empirically via the integration test as oracle.

## Consequences
- ✅ The first TV2 attempt (skipping *both* `AddPartitionsToTxn` and `AddOffsetsToTxn`) was caught by integration tests (code 48) before shipping — it would have broken all 4.x transactions.
- ✅ Established that TV2 needs **Produce v12** and that offsets registration stays explicit (a verified hybrid). See [[features/exactly-once-tv2]].

## Related
- [[features/exactly-once-tv2]] · [[packages/transactions]] · [[protocol/version-negotiation]] · [[compatibility/kafka-versions]]
- [[decisions/adr-mock-as-oracle|ADR: Real client as the mock-broker oracle]] · [[packages/kfake-mock-broker]]
