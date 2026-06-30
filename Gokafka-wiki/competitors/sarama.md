---
title: IBM/sarama
type: competitor
category: Competitors
subcategory: Client
status: reference
tags: [gokafka, competitor]
updated: 2026-06-30
url: https://github.com/IBM/sarama
license: MIT
---

# IBM/sarama

The long-standing pure-Go client. SyncProducer/AsyncProducer, Consumer/ConsumerGroup, ClusterAdmin. Explicit `Config.Version`, in-tree `mocks`, go-metrics, interceptors.

## What GoKafka adopted / matched
- `Config.Validate()`-style invariant checking (GoKafka has `config.validate()`).
- Murmur2 + selectable partitioners ([[packages/partitioners]]).
- In-tree test mocks → [[packages/kfake-mock-broker|kfake]] + `MockRegistry`.
- **DescribeUserScramCredentials** (the one SCRAM admin gap sarama had that GoKafka lacked — now closed).

## Where GoKafka differs
- No KIP-848/932; GoKafka has both.
- GoKafka negotiates versions automatically vs sarama's explicit `Version`.

## Related
[[competitors/parity-matrix]] · [[competitors/franz-go]] · [[competitors/kafka-go]] · [[protocol/version-negotiation]] · [[packages/partitioners]] · [[packages/consumer|Consumer & groups]] · [[features/next-gen-groups|Next-gen consumer groups (KIP-848)]]
