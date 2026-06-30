---
title: confluent-kafka-go
type: competitor
tags: [gokafka, competitor]
url: https://github.com/confluentinc/confluent-kafka-go
license: Apache-2.0
updated: 2026-06-30
---

# confluent-kafka-go

A **cgo wrapper over librdkafka** — mature, commercially supported, but not a single static binary and needs a C toolchain. Strongest in **Schema Registry**: full SR client, Avro/Protobuf/JSON serdes, subject-name strategies, references, GUID schemas, and **CSFLE** with cloud KMS drivers.

## What GoKafka adopted (pure Go, no cgo)
- SR **subject-name strategies** + `MockRegistry` ([[packages/schema-registry]]).
- **Header-based schema-id framing** + `SchemaByGUID`.
- **CSFLE** framework + local KMS + pluggable `KMS` interface ([[features/csfle]]).

## Where GoKafka differs
- Pure Go vs cgo/librdkafka ([[decisions/adr-stdlib-only]]).
- GoKafka leaves cloud KMS drivers to the caller (keeps core dependency-free); confluent ships them.

## Related
- [[competitors/parity-matrix]] · [[packages/schema-registry]]
