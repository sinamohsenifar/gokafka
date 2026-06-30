---
title: KIP coverage
type: protocol
category: Protocol
subcategory: Coverage
status: stable
tags: [gokafka, protocol, kip]
updated: 2026-06-30
---

# KIP coverage

Client-relevant KIPs across Kafka 3.4–4.3. Full table: `docs/CONFORMANCE.md` §2.

| KIP | Feature | Status |
|---|---|---|
| 848 | Next-gen consumer groups | ✅ [[features/next-gen-groups]] |
| 932 | Share groups / queues | ✅ [[features/share-groups]] |
| 890 | Transactions v2 (TV2) | ✅ [[features/exactly-once-tv2]] |
| 584 | Feature versioning (finalized features) | ✅ |
| 447 | `require_stable` OffsetFetch | ✅ |
| 516 | Topic IDs (Fetch v13) | ✅ |
| 482 | Flexible/tagged fields | ✅ [[protocol/flexible-encoding]] |
| 345 | Static membership | ✅ |
| 429 | Cooperative rebalance | ✅ |
| 455 | Partition reassignment | ✅ |
| 554 | SCRAM creds admin | ✅ |
| 390 | Compression level | ➖ (gzip only) |
| 320 | Leader-epoch fencing | ➖ (fencing yes; truncation detection follow-up) |
| 714 | Client metrics push | ❌ non-goal ([[decisions/adr-stdlib-only]]) |

## Related
- [[protocol/api-coverage]] · [[competitors/parity-matrix]]
- [[protocol/flexible-encoding]] · [[protocol/version-negotiation]] · [[architecture/wire-protocol]]
