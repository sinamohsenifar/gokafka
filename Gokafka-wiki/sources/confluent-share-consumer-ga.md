---
title: "Confluent — Kafka Queue Semantics Now GA with Share Consumer API"
type: source
category: Research
subcategory: KIP-932
status: reference
tags: [gokafka, source, kip-932, research]
updated: 2026-06-30
url: https://www.confluent.io/blog/kafka-queue-semantics-share-consumer-ga/
---

# Confluent — Share Consumer GA blog

Vendor synthesis; design facts corroborate the [[sources/apache-kip-932|primary spec]]. **Confidence: medium.**

## Key claims
- `KafkaShareConsumer` lets "multiple consumers cooperatively process messages from the same topic regardless of the number of partitions." Per-record **acquisition locks**, default **30s**.
- **Acknowledgement modes:** *implicit* (default — `poll()` auto-accepts the previous batch) and *explicit* (`consumer.acknowledge(record, AcknowledgeType.X)`):
  - `ACCEPT` (done), `RELEASE` (retry), `RENEW` (extend the lock for long tasks). (`REJECT` → archived, per the spec.)
- **Delivery:** configurable max (default **5**) attempts; on no-ack the lock expires and the record goes to another consumer → **at-least-once with redelivery**.
- vs consumer groups: no 1:1 partition ownership → **elastic scaling beyond partition count**.
- **Use cases:** work queues, job processing, task execution, event-driven workflows with back-pressure.
- **GA:** Apache Kafka **4.2+**, Confluent Platform **8.2**, Confluent Cloud. Java clients.

> [!gap] "Elastic scaling"/throughput benefits are stated qualitatively — no benchmark numbers; treat as vendor framing.

## Related
[[sources/apache-kip-932]] · [[Research: KIP-932 share groups (Queues for Kafka)]] · [[features/share-groups|Share groups (KIP-932)]] · [[concepts/share-group-acquisition-lock|Share-group acquisition lock & delivery count]] · [[concepts/share-coordinator-state|Share coordinator & __share_group_state]] · [[Audit: KIP-932 implementation gaps]]
