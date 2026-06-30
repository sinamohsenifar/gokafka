---
title: Flexible encoding & the decode-bug pattern
type: protocol
category: Protocol
subcategory: Encoding
status: stable
tags: [gokafka, protocol, wire, kip-482]
updated: 2026-06-30
---

# Flexible encoding & the decode-bug pattern

Kafka has two wire encodings per API version:

| | **Legacy** | **Flexible** (KIP-482) |
|---|---|---|
| Strings | int16 length + bytes | uvarint `len+1` + bytes (compact) |
| Arrays | int32 count | uvarint `count+1` (compact) |
| Tagged fields | none | trailing **tag section** on every struct + the request/response |
| Header | v1 | v2 (adds a tag section) |

`internal/wire.Buffer` provides both: `WriteString`/`ReadString` vs `WriteCompactString`/`ReadCompactString`, `WriteEmptyTagSection`/`SkipTagSection`, `ReadUvarint`, etc. Whether an API+version is flexible is decided by `flexibleRequestHeader` / `flexibleResponseHeader` in `internal/protocol/flex.go`.

> [!warning] The KIP-511 ApiVersions exception
> The ApiVersions **response header** is always non-flexible, even at v3+, so a client can parse it before it knows the broker's capabilities. `flexibleResponseHeader` returns `false` for ApiVersions specifically. [[packages/kfake-mock-broker|kfake]] reproduces this exception server-side.

## The decode-bug pattern ⚠️

A recurring class of bug: a flexible-struct decoder **omits a field** whose value is often a single `0x00` byte (a null compact string, or an empty compact array). The *next* `SkipTagSection()` reads that `0x00` as "0 tags" and silently absorbs it — so the decoder accidentally stays aligned **as long as the field is null/empty**.

It desyncs the moment a broker returns a **non-null** value for that field → "buffer too short".

This stayed latent against Apache Kafka (which returns null in the common cases) and was only surfaced by testing against [[compatibility/redpanda|Redpanda]]:

| Bug | Missing read | Latent on Kafka because… | Fix |
|---|---|---|---|
| DescribeConfigs v4 | per-**synonym** trailing tag section | fresh topics have no synonyms | v0.26.6 |
| CreatePartitions v2 | per-topic `error_message` | Kafka returns null messages | v0.26.7 |

See [[compatibility/broker-quirks]] for the full write-up.

> [!tip] Lesson
> Test against a **second** Kafka-compatible broker. Null-valued responses from one implementation hide missing-field bugs that another implementation (returning non-null) exposes.

## Related
- [[architecture/wire-protocol]] · [[protocol/version-negotiation]] · [[protocol/api-coverage]]
- [[compatibility/broker-quirks]] · [[compatibility/redpanda]] · [[architecture/transport]]
