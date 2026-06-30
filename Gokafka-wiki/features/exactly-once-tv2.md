---
title: Exactly-once / KIP-890 TV2
type: feature
category: Features
subcategory: EOS
status: ga
tags: [gokafka, feature, kip-890, eos, transactions]
updated: 2026-06-30
---

# Exactly-once / KIP-890 TV2

GoKafka implements transactional, exactly-once produce (EOS) — and the **KIP-890 transactions v2 (TV2)** produce path on modern brokers.

## TV1 (baseline)
`Producer.BeginTransaction` → `InitProducerID`, then per transaction: `AddPartitionsToTxn` (per data partition), produce, `AddOffsetsToTxn` + `TxnOffsetCommit` (for consume-transform-produce), `EndTxn`. Correct and interoperable on all brokers.

## TV2 (KIP-890), gated on `transaction.version >= 2`
Detected via `Client.BrokerFeature("transaction.version")` (the default on Kafka 4.x). When TV2 is available, the producer **skips the client-side `AddPartitionsToTxn` round-trip** — the partition leader registers data partitions implicitly on the first transactional **Produce**.

> [!important] Why Produce v12
> The broker only does the implicit partition-add at **Produce v12+**. At v9 it returns `INVALID_TXN_STATE`. So enabling TV2 required bumping Produce v9 → v12. Determined empirically — the integration test (code 48 vs success) was the oracle.

> [!note] Offsets stay explicit
> The consumer-group offsets registration keeps `AddOffsetsToTxn` even under TV2 — unlike data partitions, that registration is **not** implicit on `TxnOffsetCommit` (broker returns `INVALID_TXN_STATE` otherwise). A verified hybrid.

## Epoch handling
The producer epoch is bumped server-side on `EndTxn`. GoKafka adopts the bumped epoch (EndTxn v5) / re-inits per transaction, so it always uses a valid epoch.

## Supporting correctness
- `read_committed` filters aborted records via the aborted-txn list on the Fetch path.
- `require_stable=true` on OffsetFetch v7 (KIP-447) — a resuming EOS consumer never reads a stale committed offset. See [[protocol/version-negotiation]].
- `ErrCodeTransactionAbortable` (120) is a named, non-retriable error.

## Related
[[packages/transactions]] · [[packages/producer]] · [[decisions/adr-tv2-incremental]] · [[concepts/epoch-reconciliation]] · [[protocol/version-negotiation]] · [[competitors/parity-matrix]] · [[features/consumer-lag]]
