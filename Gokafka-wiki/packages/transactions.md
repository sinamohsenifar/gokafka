---
title: Transactions / EOS
type: package
category: Packages
subcategory: Transactions
status: stable
tags: [gokafka, package, eos, transactions, kip-890]
updated: 2026-06-30
---

# Transactions / EOS

Root `transaction.go`. `Producer.BeginTransaction` → `TransactionalProducer` with `ProduceWithinTxn`, `SendOffsetsToTxn` (consume-transform-produce), `Commit`, `Abort`.

Implements both **TV1** and **KIP-890 TV2** — gated on `transaction.version` ([[features/exactly-once-tv2]]). Patient `coordinatorRetry` handles cold transaction-coordinator startup.

## Related
- [[features/exactly-once-tv2]] · [[decisions/adr-tv2-incremental]] · [[packages/consumer]]
- [[packages/producer]] · [[packages/admin]] · [[decisions/adr-tv2-incremental]]
- [[competitors/parity-matrix]] · [[architecture/cluster-coordinator]]
