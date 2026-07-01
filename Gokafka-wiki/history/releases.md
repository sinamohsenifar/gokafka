---
title: Release history
type: history
category: History
subcategory: Releases
status: active
tags: [gokafka, history, releases, changelog]
updated: 2026-07-01
---

# Release history

Source of truth: `CHANGELOG.md`. Highlights below.

## 0.26.22–0.26.27 — data-loss / message-integrity audit
6 confirmed integrity bugs found by an adversarial find→verify Workflow, each fixed with a regression test and real-broker verification.
| Version | Theme |
|---|---|
| **0.26.27** | Per-partition produce retry — partial multi-broker failure no longer re-sends (dups) committed partitions (idempotence off); real 3-broker validated |
| **0.26.26** | `Poll` delivered-position cursor — fixes silent multi-broker over-fetch loss (`out[:maxPoll]` dropped already-bumped records) + no-arg `Commit` fetch-position |
| **0.26.25** | Freeze partition assignment across produce retries (RoundRobin re-partition → reorder/cross-partition dup) |
| **0.26.24** | `OffsetFetch` completeness — never resume an assigned partition at offset 0 on a transient/omitted response (mass re-read) |
| **0.26.23** | Idempotent sequence reserved per-record (not per-batch) + `OUT_OF_ORDER_SEQUENCE`/epoch made fatal (were retriable + reset-PID → dup) |
| **0.26.22** | Record-batch CRC-32C validation on consume (was read+discarded → corrupt batch silently skipped records) |

## 0.26.8–0.26.21 — hardening, perf, KIP-932 admin
| Version | Theme |
|---|---|
| **0.26.19–0.26.21** | Decoder perf (2014→4 allocs record-batch), producer send-path alloc trim, fuzz-hardened wire decoders |
| **0.26.12–0.26.17** | KIP-932 share-group admin (offsets/lag/configs, API 90/91/92), `delivery_count`, rebootstrap resilience, `kfake.NewBrokerAt` |
| **0.26.8–0.26.11** | KIP-932 ack modes + share-consumer robustness; Protobuf/SR completeness fixes |

## 0.26.x — mock broker, CSFLE, Redpanda
| Version | Theme |
|---|---|
| **0.26.7** | [[compatibility/redpanda\|Redpanda]] CI lane + CreatePartitions decode fix + portable suite |
| **0.26.6** | Redpanda protocol compat: DescribeConfigs synonyms, v0 negotiation, clear unsupported-API error ([[compatibility/broker-quirks]]) |
| **0.26.4** | `DescribeLogDirs` partial results on per-broker failure (franz-go "shard errors") |
| **0.26.3** | Header-based SR framing + `SchemaByGUID` |
| **0.26.2** | Partition reassignment admin (KIP-455, API 45/46) |
| **0.26.1** | [[features/csfle\|CSFLE]] field-level encryption (pure Go) |
| **0.26.0** | [[packages/kfake-mock-broker\|kfake]] in-process mock broker |

## 0.25.x — competitor parity + KIP-890 TV2
| Version | Theme |
|---|---|
| **0.25.26** | `DescribeUserScramCredentials` (API 50) |
| **0.25.25** | OffsetFetch v7 `require_stable` (EOS, KIP-447) |
| **0.25.24** | Clear TLS-mismatch error |
| **0.25.23** | SR subject strategies + `MockRegistry` |
| **0.25.22** | `Admin.ConsumerGroupLag` |
| **0.25.21** | CRC32 + custom partitioners |
| **0.25.14** | [[features/exactly-once-tv2\|KIP-890 TV2]] produce path (Produce v12) |
| **0.25.13** | Cluster feature negotiation (KIP-584 / TV2 foundation) |

## Process
Every change: branch → PR → full CI matrix (Kafka 3.9.2–4.3.0 × Go 1.22–1.26 + Redpanda lane) → merge → tag → release. Committed as Sina Mohsenifar only, no co-authors.

## Related
[[log|Activity log]] · [[competitors/parity-matrix]] · [[compatibility/kafka-versions]] · [[compatibility/redpanda]] · [[features/csfle]] · [[features/exactly-once-tv2]] · [[packages/kfake-mock-broker]] · [[protocol/api-coverage]]
