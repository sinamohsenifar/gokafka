---
title: Kafka 3.9–4.3
type: compatibility
tags: [gokafka, compatibility, kafka]
updated: 2026-06-30
---

# Kafka 3.9–4.3

Every release is CI-tested against Apache Kafka **3.9.2, 4.0.2, 4.1.2, 4.2.1, 4.3.0** across **Go 1.22–1.26** (matrix in `docs/COMPATIBILITY.md`). A single client binary spans all of them via [[protocol/version-negotiation|version negotiation]].

- KRaft-only (ZooKeeper paths removed); only v2 record batches (KIP-896).
- TV2 transactions on 4.x; falls back to TV1 on older brokers.
- KIP-848 / KIP-932 require 4.1+ with the relevant feature finalized.

Also CI-tested against [[compatibility/redpanda|Redpanda]].

## Related
- [[compatibility/redpanda]] · [[protocol/version-negotiation]]
