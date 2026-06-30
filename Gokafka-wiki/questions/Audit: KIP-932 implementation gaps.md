---
title: "Audit: KIP-932 implementation gaps"
type: audit
category: Research
subcategory: Audit
status: resolved
tags: [gokafka, audit, kip-932, share-groups]
updated: 2026-06-30
related:
  - "[[Research: KIP-932 share groups (Queues for Kafka)]]"
  - "[[features/share-groups]]"
method: "35-agent ultracode Workflow — 5 dimensions audited, every claimed gap adversarially re-verified against the code (25 confirmed, 4 dismissed as false alarms)"
---

# Audit: KIP-932 implementation gaps

GoKafka's share-group (KIP-932) implementation audited against the [[Research: KIP-932 share groups (Queues for Kafka)|GA spec]]. **Verdict:** all four client RPCs (76–79) are wire-correct with Renew, but the implementation is **not GA-complete** — a real ack-error correctness bug, no implicit/explicit ack mode, no settable share group-configs, dropped observability fields, and no Release/Reject/error-recovery test coverage.

## Confirmed gaps (by severity)

### HIGH
1. ✅ **FIXED (v0.26.8)** — ShareAcknowledge decoder ignored per-partition ack errors → a failed ack reported as success. Now walks the full topic→partition structure (version-aware v1/v2), surfacing the first non-zero code. Wire layout verified against a captured real 4.1.2 (v1) response + the Apache 4.3 schema (v2); regression-tested; full CI matrix green.
2. ✅ **FIXED (v0.26.9)** — `share.acknowledgement.mode` (implicit vs explicit) now plumbed via `WithShareAcknowledgementMode(ShareAckExplicit|ShareAckImplicit)`. Implicit auto-accepts the previous Poll's batch on the next Poll (or on Leave); explicit is the default. Tracked in `pendingAccept`, pruned on terminal ack (Renew keeps in flight).
3. ✅ **FIXED (v0.26.9)** — Release/retry path now covered by `TestIntegrationShareReleaseRedelivers` (Released record returns to the group and is redelivered).
4. ✅ **FIXED (v0.26.9)** — Reject path now covered by `TestIntegrationShareRejectNoRedelivery` (Rejected record archived, not redelivered).

**Bonus (v0.26.9):** making the Release/Reject/implicit tests deterministic surfaced a real **share-consumer connection-robustness** class of bug — the foreground Poll/ack path and the background heartbeat share the per-broker connection (leader == coordinator on a single-broker cluster), so a Poll-timeout-mid-fetch, a concurrent invalidation, or a Leave racing the heartbeat could close a connection under an in-flight request (`use of closed network connection` / `i/o timeout`) and in the worst case fence the member and redeliver already-accepted records. Fixed three ways: Poll clamps each ShareFetch's broker-side wait to the caller's remaining deadline; `Cluster.Request` re-dials+resends once on a not-sent (write-failed) conn error (`transport.ErrNotSent`); `Leave`/`stopShareHeartbeat` wait for the heartbeat goroutine to exit first. Stress-tested 20× + full broker matrix green.

### MEDIUM
- ✅ **FIXED (v0.26.10)** — No unsupported-broker guard in the ShareConsumer hot path (asymmetric with Admin) → opaque error on Redpanda / no-`share.version` brokers. `Poll` now returns a clear "broker does not support KIP-932 share groups … requires Apache Kafka 4.1+ with share.version >= 1" error; share API keys 76–79 named in `protocol.APIName`; integration test on the Redpanda + 3.9.2 lanes.
- Acks never piggybacked on ShareFetch (extra round-trip; encoder already supports it).
- ✅ **FIXED (v0.26.13)** — `share.isolation.level` not settable → now applied (`read_committed` when the consumer isolation level is ReadCommitted) on join via the GROUP-config path.
- ✅ **FIXED (v0.26.13)** — `share.auto.offset.reset` only half-exposed → new `WithShareAutoOffsetReset("earliest"|"latest"|"by_duration:…")`; `WithConsumeFromBeginning` still forces earliest.
- ✅ **FIXED (v0.26.13)** — **No public GROUP-config write path** (the root cause) → `Admin.AlterGroupConfigs` / `DescribeGroupConfigs` on the GROUP resource (type 32); `applyShareStartOffset` generalized to `applyShareGroupConfigs`. Verified end-to-end (`TestIntegrationAdminGroupConfigs`).
- ✅ **FIXED (v0.26.12)** — `delivery_count` decoded then discarded → now surfaced on `Record.DeliveryCount` (1 on first delivery, +1 per re-acquire), for DLQ logic. Verified end-to-end.
- No decode/round-trip unit tests for any share response; Poll-retry, session-error (122/123), rejoin/heartbeat/failover untested.

### LOW
- ShareAcknowledge decode loses `error_message`; ShareFetch piggyback ack diagnostics discarded; `AcquisitionLockTimeoutMs` never surfaced; no Admin for the share-offset RPCs (~85/86/87/90); acks not coalesced into ranges; Renew v-gate uses non-zero fallback; `Run` untested.

## Not gaps (verified — do not re-file)
`Poll` is the deliberate public verb (no `Fetch` alias); read_committed handling is correct (server-side filtering); multi-consumer-per-partition is broker-enforced; server-side share-coordinator/`__share_group_state` machinery correctly not client-side; lock-duration/delivery-limit/max-locks correctly not client-sent.

## Fix order
1. `internal/protocol/share_acknowledge.go` — per-partition errors (the bug) + `error_message`.
2. ShareConsumer unsupported-broker guard + Renew v-gate fallback.
3. Implicit/explicit ack mode + `share.*` config exposure (needs a public GROUP-config write path).
4. Release/Reject integration tests + decoder unit tests.

## Related
- [[Research: KIP-932 share groups (Queues for Kafka)]] · [[features/share-groups|Share groups (KIP-932)]] · [[concepts/share-group-acquisition-lock|Share-group acquisition lock & delivery count]]
- [[concepts/share-coordinator-state|Share coordinator & __share_group_state]] · [[sources/apache-kip-932|Apache cwiki KIP-932]] · [[protocol/api-coverage|API coverage]]
- [[compatibility/redpanda|Redpanda compatibility]] · [[packages/consumer|Consumer & groups]]
