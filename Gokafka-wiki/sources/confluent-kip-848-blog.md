---
title: "Confluent — KIP-848: A New Consumer Rebalance Protocol for Apache Kafka 4.0"
type: source
category: Research
subcategory: KIP-848
status: reference
tags: [gokafka, source, kip-848, research]
updated: 2026-06-30
url: https://www.confluent.io/blog/kip-848-consumer-rebalance-protocol/
---

# Confluent — KIP-848 blog

Vendor synthesis. **Confidence: medium** (vendor; design facts corroborate the [[sources/apache-kip-848|primary spec]], performance claims unverified).

## Key claims
- The classic **eager** strategy is "stop-the-world": any membership or metadata change halts the whole group. Even **cooperative** rebalancing still needs a group-wide sync barrier + client-driven logic — slow for large groups.
- New protocol is **server-driven**: the coordinator holds group state and computes assignments; consumers declare subscriptions and ack assign/revoke via heartbeat.
- **Incremental reconciliation:** a consumer pauses only the partitions it's told to revoke; others keep processing. No global barrier; commits and fetches continue during rebalance.
- Config: consumers set `group.protocol=consumer` and optionally `group.remote.assignor`. Session-timeout/heartbeat configs move **server-side**.
- Upgrade: Apache Kafka 4.0+ / Confluent Platform 8.0+ / Confluent Cloud; clients need compatible libs (e.g. **librdkafka 2.10+**, early access). Live rolling upgrades supported with automatic proxying for classic clients.

> [!gap] The blog claims "faster rebalances" and "reduced/eliminated downtime" but provides **no timing numbers** — treat performance magnitudes as low confidence until independently measured.

## Related
[[sources/apache-kip-848]] · [[Research: KIP-848 next-gen consumer rebalance protocol]] · [[features/next-gen-groups]] · [[concepts/server-side-assignor]] · [[concepts/epoch-reconciliation]] · [[concepts/consumergroupheartbeat]] · [[sources/kafka-docs-rebalance-protocol]]
