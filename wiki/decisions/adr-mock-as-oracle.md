---
title: "ADR: Real client as the mock-broker oracle"
type: decision
status: accepted
tags: [gokafka, adr, decision, testing]
updated: 2026-06-30
---

# ADR: Real client as the mock-broker oracle

## Status
Accepted (for [[packages/kfake-mock-broker|kfake]]).

## Context
Building an in-process mock Kafka broker means implementing the *server* side of the wire protocol — the inverse of the client codecs. There is no second reference implementation to check it against without spinning up real Kafka (which defeats the purpose).

## Decision
Use the **real GoKafka client as the correctness oracle**. kfake advertises narrow ApiVersions ranges so the client negotiates down to the exact versions kfake implements; tests then drive full produce→consume→commit→lag and admin flows through the real client against the mock. If kfake's encoding is wrong, the client's own decoders reject it.

## Consequences
- ✅ No external dependency to validate the mock; every handler is exercised end-to-end.
- ✅ kfake stays small by mirroring only the negotiated versions, and by storing record batches **opaquely** (patch base offset, serve verbatim) instead of parsing records.
- ⚠️ kfake is single-node, in-memory, not durable — test-only.
- ⚠️ Bugs symmetric in both client and mock could hide; mitigated by also running the suite against **real Kafka and [[compatibility/redpanda|Redpanda]]**.

## Related
- [[packages/kfake-mock-broker]] · [[protocol/flexible-encoding]]
