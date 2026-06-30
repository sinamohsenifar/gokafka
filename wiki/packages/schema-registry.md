---
title: Schema Registry & serdes
type: package
tags: [gokafka, schema-registry, serde]
updated: 2026-06-30
---

# Schema Registry & serdes

`schema/` package — a Confluent-compatible Schema Registry client + serdes (pure Go, stdlib HTTP).

- **Registry** — register, `SchemaByID`, **`SchemaByGUID`**, compatibility, config, subjects/versions, lifecycle (soft/permanent delete).
- **Subject strategies** — `SubjectForTopic` (TopicNameStrategy), `SubjectForRecord` (RecordNameStrategy), `SubjectForTopicRecord` (TopicRecordNameStrategy) + pluggable `SubjectNameStrategy`.
- **Serde** — Avro/JSON/Protobuf with Confluent wire framing (magic `0x00` + 4-byte id). **Header-based framing** (`EncodeAvroHeaderFramed`, `SchemaIDHeaderKey`) carries the id in a record header instead.
- **`SchemaClient` interface** — `*Registry` or **`MockRegistry`** (in-memory, for tests with no live registry).
- **CSFLE** — [[features/csfle|field-level encryption]] (`FieldEncrypter`, `KMS`, `LocalKMS`).

Works against Confluent SR, Apicurio ccompat, and [[compatibility/redpanda|Redpanda's]] built-in SR.

## Related
- [[features/csfle]] · [[competitors/confluent-kafka-go]]
