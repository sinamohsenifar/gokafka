---
title: Release history
type: history
category: History
subcategory: Releases
status: active
tags: [gokafka, history, releases, changelog]
updated: 2026-06-30
---

# Release history

Source of truth: `CHANGELOG.md`. Highlights below.

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
