---
title: segmentio/kafka-go
type: competitor
tags: [gokafka, competitor]
url: https://github.com/segmentio/kafka-go
license: MIT
updated: 2026-06-30
---

# segmentio/kafka-go

Pure-Go, ergonomics-first. Three tiers: `Conn` (low-level), `Reader`/`Writer` (high-level), `Client` (admin). `context.Context`-first API. No transactions/EOS, no idempotent producer.

## What GoKafka adopted
- Cross-client **CRC32** balancer (librdkafka-compatible) → [[packages/partitioners|CRC32Partitioner]].
- Its documented **TLS-vs-plaintext footgun** (opaque `io.ErrUnexpectedEOF`) → GoKafka's clear TLS-mismatch hint.

## Where GoKafka differs
- GoKafka has idempotence, transactions/EOS, KIP-848/932, a full admin surface, and mocks — kafka-go has none of these.

## Related
- [[competitors/parity-matrix]]
