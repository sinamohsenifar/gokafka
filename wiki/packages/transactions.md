---
title: Transactions / EOS
type: package
tags: [gokafka, transactions, eos]
updated: 2026-06-30
---

# Transactions / EOS

Root `transaction.go`. `Producer.BeginTransaction` → `TransactionalProducer` with `ProduceWithinTxn`, `SendOffsetsToTxn` (consume-transform-produce), `Commit`, `Abort`.

Implements both **TV1** and **KIP-890 TV2** — gated on `transaction.version` ([[features/exactly-once-tv2]]). Patient `coordinatorRetry` handles cold transaction-coordinator startup.

## Related
- [[features/exactly-once-tv2]] · [[decisions/adr-tv2-incremental]] · [[packages/consumer]]
