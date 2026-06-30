---
title: Compression
type: package
category: Packages
subcategory: Producer
status: stable
tags: [gokafka, package, compression]
updated: 2026-06-30
---

# Compression

`internal/compress/` — **gzip, snappy, lz4, zstd**, all pure Go (no CGO). Selected via `WithProducerCompression`; decompression is auto-detected from the record-batch attributes on fetch.

- **gzip** honors `WithProducerCompressionLevel` (KIP-390, clamp 1–9).
- snappy/lz4/zstd are fixed-strategy and ignore the level.

## Related
[[packages/producer]] · [[packages/consumer]] · [[decisions/adr-stdlib-only]] · [[architecture/transport]] · [[architecture/wire-protocol]] · [[competitors/parity-matrix]]
