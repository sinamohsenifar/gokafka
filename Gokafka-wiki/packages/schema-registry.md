---
title: Schema Registry & serdes
type: package
category: Packages
subcategory: Schema
status: stable
tags: [gokafka, package, schema-registry, serde]
updated: 2026-06-30
---

# Schema Registry & serdes

`schema/` package — a Confluent-compatible Schema Registry client + serdes (pure Go, stdlib HTTP).

- **Registry** — register, `SchemaByID`, **`SchemaByGUID`**, compatibility, config, subjects/versions, lifecycle (soft/permanent delete), **schema references** (`RegisterWithReferences`, `IsCompatibleWithReferences`, `IsRegisteredWithReferences`) for multi-file `.proto` imports / reused Avro-JSON types.
- **Subject strategies** — `SubjectForTopic` (TopicNameStrategy), `SubjectForRecord` (RecordNameStrategy), `SubjectForTopicRecord` (TopicRecordNameStrategy) + pluggable `SubjectNameStrategy`.
- **Serde** — Avro/JSON/Protobuf with Confluent wire framing (magic `0x00` + 4-byte id; Protobuf adds the zigzag-varint message-index array). **Header-based framing** (`EncodeAvroHeaderFramed`, `SchemaIDHeaderKey`) carries the id in a record header instead. Decode applies wire-schema-id guards (`ExpectedSchemaID`/`AllowedSchemaIDs`/`PinRegisteredSchemaID`) on both Avro and Protobuf.
- **`SchemaClient` interface** — `*Registry` or **`MockRegistry`** (in-memory, for tests with no live registry).
- **CSFLE** — [[features/csfle|field-level encryption]] (`FieldEncrypter`, `KMS`, `LocalKMS`).

> [!note] Protobuf message codec is BYO (by design)
> GoKafka owns the Confluent **wire framing + registry integration** for Protobuf (verified end-to-end vs a real registry and against Confluent golden bytes). It does **not** marshal a Go value to Protobuf — `EncodeProtobuf` wraps **pre-encoded** bytes, which the app produces with `google.golang.org/protobuf`. A built-in codec needs that runtime (or `.proto` codegen), which would break [[decisions/adr-stdlib-only|stdlib-only]] — the same dependency every Go Kafka client actually relies on; GoKafka just leaves that one import to the application. JSON-Schema is similarly framed-but-not-validated for the same reason.

Works against Confluent SR, Apicurio ccompat, and [[compatibility/redpanda|Redpanda's]] built-in SR. See [[Audit: Protobuf & Schema Registry completeness]] for the code-verified completeness audit.

## Related
[[features/csfle]] · [[competitors/confluent-kafka-go]] · [[Audit: Protobuf & Schema Registry completeness]] · [[packages/producer]] · [[packages/consumer]] · [[decisions/adr-stdlib-only|ADR: Stdlib-only]] · [[compatibility/redpanda|Redpanda compatibility]]
