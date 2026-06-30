---
title: Architecture overview
type: architecture
category: Architecture
subcategory: Overview
status: stable
tags: [gokafka, architecture]
updated: 2026-06-30
---

# Architecture overview

GoKafka is layered: a small **public API** (root package `gokafka`) over **internal** subsystems that speak the Kafka wire protocol. No third-party modules; only the Go standard library.

```
gokafka (public)                Client, Producer, Consumer, ShareConsumer, Admin
  ├─ schema/                    Schema Registry client, Serde, MockRegistry, CSFLE
  ├─ kfake/                     in-process mock broker (test-only)
  ├─ observe/, metrics/         pluggable logging/tracing/metrics
  └─ internal/
       ├─ broker/  (Cluster)    metadata, leader/coordinator resolution, version negotiation
       ├─ transport/ (Conn)     TCP framing, request/response, SASL/TLS dial
       ├─ protocol/             per-API encode/decode, API keys, KIP logic
       ├─ wire/  (Buffer)       primitive read/write, compact/varint, tagged fields
       ├─ produce/, compress/   record batching, codecs
       └─ auth/                 SASL (PLAIN/SCRAM/OAUTHBEARER/GSSAPI), TLS config
```

## Request path
1. `gokafka.NewClient` → [[architecture/client-lifecycle|connect]]: `Cluster.NegotiateVersions` (ApiVersions) then `Cluster.Refresh` (Metadata).
2. A public call (e.g. `Producer.ProduceSync`) builds a request body via `internal/protocol`, picks the **negotiated** API version, and routes through [[architecture/cluster-coordinator|Cluster]] to the right broker.
3. [[architecture/transport|Transport]] frames the request, writes it, reads the response, and strips the response header (flexible or not — see [[architecture/wire-protocol]]).
4. `internal/protocol` decodes the response body.

## Key design properties
- **Stdlib-only** — see [[decisions/adr-stdlib-only]].
- **Version negotiation** — the client adapts to whatever the broker advertises ([[protocol/version-negotiation]]), which is why it works across Kafka 3.9–4.3 and [[compatibility/redpanda|Redpanda]].
- **Flexible vs legacy encoding** is handled per-API and per-version ([[protocol/flexible-encoding]]).

## Related
- 📄 Canonical doc: [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md)
- [[packages/producer]] · [[packages/consumer]] · [[packages/admin]] · [[packages/transactions]]
- [[packages/kfake-mock-broker]] mirrors this stack server-side for tests.
- [[architecture/client-lifecycle]] · [[architecture/cluster-coordinator]] · [[architecture/transport]] · [[architecture/wire-protocol]]
- [[protocol/api-coverage]] · [[decisions/adr-stdlib-only]]
