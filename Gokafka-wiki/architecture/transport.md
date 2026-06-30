---
title: "Transport: framing & connections"
type: architecture
category: Architecture
subcategory: Transport
status: stable
tags: [gokafka, architecture, transport]
updated: 2026-06-30
---

# Transport: framing & connections

`internal/transport/conn.go` — one TCP connection, one in-flight RPC at a time.

- **Request frame**: 4-byte length prefix + request header (`EncodeRequest`) + body. Header is v2 (flexible, with tag section) when `flexibleRequestHeader` is true ([[protocol/flexible-encoding]]); ClientId stays a legacy string even then.
- **Response**: read 4-byte size, then the frame; `ResponseBodyForAPI` strips the correlation id and (for flexible APIs) the response-header tag section — **except ApiVersions** (KIP-511, never flexible response header).
- **Dial**: `auth.Dial` handles TLS + SASL (PLAIN/SCRAM/OAUTHBEARER/GSSAPI); `Reauthenticate` refreshes OAuth tokens (KIP-368).

## Related
[[architecture/wire-protocol]] · [[architecture/cluster-coordinator]] · [[protocol/flexible-encoding]] · [[protocol/version-negotiation]] · [[architecture/client-lifecycle]] · [[architecture/overview]]
