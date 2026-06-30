---
title: "Wire protocol: flexible vs legacy"
type: architecture
category: Architecture
subcategory: Wire
status: stable
tags: [gokafka, architecture, wire]
updated: 2026-06-30
---

# Wire protocol: flexible vs legacy

`internal/wire/buffer.go` — the byte-level primitive layer used by every codec.

- Legacy: `WriteString`/`ReadString` (int16 len), `WriteInt8/16/32/64`, int32 arrays.
- Flexible (KIP-482): `WriteCompactString`/`ReadCompactString` (uvarint `len+1`), `WriteCompactArrayLen`, `ReadUvarint`, `WriteEmptyTagSection`/`SkipTagSection`, `ReadCompactNullableString`, `WriteUUID` (topic ids).

`internal/protocol/*.go` builds requests and parses responses on top, branching legacy vs flexible per API/version.

> See [[protocol/flexible-encoding]] for the **decode-bug pattern** that null-valued fields hide.

## Related
[[architecture/transport|Transport]] · [[protocol/flexible-encoding|Flexible encoding]] · [[protocol/api-coverage|API coverage]] · [[protocol/version-negotiation|Version negotiation]] · [[architecture/overview|Architecture overview]] · [[compatibility/broker-quirks|Broker quirks & decode bugs]]
