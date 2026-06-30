---
title: Observability
type: package
category: Packages
subcategory: Observability
status: stable
tags: [gokafka, package, observability, metrics]
updated: 2026-06-30
---

# Observability

`observe/` + `metrics/`. Pluggable, dependency-free hooks:

- **Logger** — incl. `WithSlogLoggerFrom`/`WithSlogHandler` to route into an app's existing `slog`; ECS-style fields.
- **Tracer** — span hooks (OpenTelemetry bridge available).
- **MetricsRecorder** — counters/gauges/histograms; built-in `Collector` accumulates produced/consumed/error/byte counts, exposed via `Client.Metrics()` snapshot. Prometheus + OTel bridges.

> KIP-714 client-metrics *push* is a deliberate non-goal (OTLP/protobuf vs [[decisions/adr-stdlib-only|stdlib-only]]); this is the alternative.

## Related
[[decisions/adr-stdlib-only]] · [[competitors/parity-matrix]] · [[packages/producer]] · [[packages/consumer]] · [[architecture/transport]] · [[features/consumer-lag]]
