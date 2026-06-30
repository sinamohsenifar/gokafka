---
title: franz-go
type: competitor
tags: [gokafka, competitor]
url: https://github.com/twmb/franz-go
license: BSD-3-Clause
updated: 2026-06-30
---

# franz-go

The pure-Go parity bar. Feature-complete (170+ KIPs), fast. Packages: `kgo` (client), `kmsg` (codegen protocol), `kadm` (admin), `kfake` (mock broker), `sr` (schema registry), plugins (`kprom`, `kzap`).

## What GoKafka adopted from it
- [[packages/kfake-mock-broker|kfake]] in-process mock broker concept.
- `kadm`-style [[features/consumer-lag|consumer lag]].
- Transaction correctness invariants → OffsetFetch **`require_stable`** ([[features/exactly-once-tv2]]).
- "Shard errors" partial multi-broker admin results (`DescribeLogDirs`).

## Where GoKafka differs
- Stdlib-only ([[decisions/adr-stdlib-only]]) vs franz-go's Go-module deps.
- GoKafka adds pure-Go [[features/csfle|CSFLE]] (franz-go has none).

## Related
- [[competitors/parity-matrix]]
