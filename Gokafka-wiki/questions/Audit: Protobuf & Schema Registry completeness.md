---
title: "Audit: Protobuf & Schema Registry completeness"
type: audit
category: Research
subcategory: Audit
status: actionable
tags: [gokafka, audit, protobuf, schema-registry]
updated: 2026-06-30
related:
  - "[[packages/schema-registry]]"
  - "[[features/csfle]]"
  - "[[decisions/adr-stdlib-only]]"
method: "Direct code read of schema/serde.go, schema/registry.go, internal/schema/wire/confluent.go + ran unit tests and serde benchmarks + grepped integration coverage. Findings are verified against code/tests, not claimed."
---

# Audit: Protobuf & Schema Registry completeness

Empirical verification (not a claim) of whether GoKafka "supports Protobuf completely" and whether Schema Registry + serde features actually work and perform. Method: read the code, run the tests, run the benchmarks.

## Verdict

GoKafka implements the **Confluent Protobuf wire format and Schema-Registry integration completely and interoperably**, but does **not** include a Protobuf *message* codec — by design (stdlib-only). Two real, stdlib-fixable gaps exist (schema references; no end-to-end Protobuf integration test) plus a minor decode-guard inconsistency.

## Verified COMPLETE (high confidence)

- **Confluent Protobuf wire framing** — magic `0x00` + big-endian schema id + message-index array (zigzag varints; `[0]` collapses to a single `0` byte). **Unit-tested against Confluent `KafkaProtobufSerializer` golden bytes** (`internal/schema/wire/confluent_test.go` `TestProtobufIndexGoldenBytes`). Wire-interoperable with Confluent / Redpanda / Apicurio. (`internal/schema/wire/confluent.go:61`)
- **Protobuf schema registration** — `RegisterProtobuf` posts `schemaType=PROTOBUF`. (`schema/registry.go:92`)
- **Avro** — full pure-Go codec (`internal/avro`), Confluent + header framing; **integration-tested** vs Apicurio ccompat; **benchmarked 57.9 ns/op, 1 alloc**.
- **JSON / JSON-Schema** — stdlib `encoding/json` + framing; **benchmarked 203 ns/op**.
- **Registry client endpoints** — register (Avro/JSON/Protobuf), `SchemaByID`, `SchemaByGUID`, `ListSubjects`, `ListVersions`, `SchemaByVersion`, `IsCompatible`, `Compatibility`/`SetCompatibility`, `Mode`/`SetMode`, `IsRegistered`, `DeleteSubjectVersion`, `DeleteSubject`.
- **Subject strategies** (TopicName / RecordName / TopicRecordName + pluggable), **`MockRegistry`** for offline serde round-trips, **CSFLE** field encryption.

## Gaps (verified — file:line)

### Real, stdlib-fixable — ✅ ALL FIXED (v0.26.11)
1. ✅ **FIXED** — schema references. Added `Reference` type + `RegisterWithReferences` / `IsCompatibleWithReferences` / `IsRegisteredWithReferences` (+ `SerdeConfig.References`, `SubjectVersion.References`); the `references` array is now sent on the register/compat/lookup bodies. Multi-file `.proto` imports and reused Avro/JSON types register. httptest unit test asserts the array is on the wire.
2. ✅ **FIXED** — end-to-end Protobuf integration test. `TestIntegrationSchemaProtobufRoundTrip` registers a `.proto` (schemaType=PROTOBUF) against the real ccompat registry, frames pre-encoded bytes, and decodes back the indexes + payload. **Verified passing against a live registry** — no longer a claim.
3. ✅ **FIXED** — Protobuf decode now applies the same wire-schema-id guards as Avro (`ExpectedSchemaID`/`AllowedSchemaIDs`/`PinRegisteredSchemaID`), shared via `checkWireSchemaID`. Unit-tested.

### Hard stdlib boundaries (document, do not implement)
4. **No Protobuf message codec.** `EncodeProtobuf(schema, protoPayload []byte)` wraps **pre-encoded** Protobuf bytes; GoKafka does not marshal a Go value → Protobuf (`serde.go:224`). A real codec needs `google.golang.org/protobuf` (descriptors/reflection) or `.proto` codegen — both third-party, which violates [[decisions/adr-stdlib-only|stdlib-only]]. This is the *same* dependency every Go Kafka client (confluent-kafka-go, franz-go) actually relies on for Protobuf; GoKafka leaves that one import to the application and owns the wire/registry layers. **BYO-bytes is the correct boundary — document it explicitly.**
5. **JSON Schema is not validated.** `FormatJSONSchema` registers the schema but encode/decode is plain `json.Marshal`/`Unmarshal` with no validation against it (`serde.go:202`). A validator is third-party or a large hand-roll — stdlib-blocked like (4).

## Performance (measured, not claimed)
```
BenchmarkSerdeEncodeAvro-8   22090102   57.89 ns/op   64 B/op   1 allocs/op
BenchmarkSerdeEncodeJSON-8    5586421   203.1 ns/op   96 B/op   4 allocs/op
```
No Protobuf benchmark exists (framing-only; the message codec is the app's). Avro/JSON serde paths are fast and low-alloc.

## Fix order
1. Schema references (gap 1) — biggest real interop win; unblocks multi-file `.proto`.
2. End-to-end Protobuf integration test (gap 2) — turns "Protobuf works" from claim to verified.
3. Protobuf decode schema-id guards (gap 3) — parity with Avro decode.
4. Document the BYO-Protobuf + JSON-Schema-validation boundaries (gaps 4, 5) in `packages/schema-registry` + README.

## Related
- [[packages/schema-registry]] · [[features/csfle]] · [[decisions/adr-stdlib-only]] · [[competitors/confluent-kafka-go]]
- [[competitors/parity-matrix]] · [[competitors/franz-go]] · [[compatibility/redpanda]]
