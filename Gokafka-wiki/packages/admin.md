---
title: Admin
type: package
category: Packages
subcategory: Admin
status: stable
tags: [gokafka, package, admin]
updated: 2026-06-30
---

# Admin

Root `admin*.go`. **43 client-facing API keys** ([[protocol/api-coverage]]). Surface:

- **Topics** — Create/Delete/ListTopics, DescribeTopicConfigs, CreatePartitions, Alter/IncrementalAlterConfigs.
- **Partitions** — ElectLeaders, **Alter/ListPartitionReassignments** (KIP-455), DeleteRecords.
- **Groups** — Describe/List/DeleteGroups, AlterConsumerGroupOffsets, **`ConsumerGroupLag`** ([[features/consumer-lag]]), FetchOffsets (multi-group, KIP-709).
- **ACLs** — Create/Describe/DeleteAcls.
- **Security** — Alter/**DescribeUserScramCredentials** (KIP-554).
- **Quotas** — Describe/AlterClientQuotas (KIP-546).
- **Transactions** — List/DescribeTransactions (KIP-664).
- **Cluster** — DescribeCluster, DescribeLogDirs (partial results on per-broker failure).

> Unsupported APIs return a clear `"broker does not support API key N"` error ([[compatibility/broker-quirks]]).

## Related
[[protocol/api-coverage]] · [[features/consumer-lag]] · [[compatibility/redpanda]] · [[packages/consumer]] · [[packages/transactions]] · [[architecture/cluster-coordinator]] · [[competitors/parity-matrix]]
