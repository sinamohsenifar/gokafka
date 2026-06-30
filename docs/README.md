# GoKafka Documentation

Everything beyond the [main README](../README.md). Start there for installation and the
quick-start; come here for depth.

> The same knowledge is also available as a navigable **Obsidian** knowledge map under
> [`wiki/`](../wiki/index.md) — open that folder as a vault for graph view, backlinks,
> and dynamic Bases.

## Understand the design
| Doc | What's inside |
|---|---|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Layered design, request path, flexible-vs-legacy encoding, version negotiation, subsystems, design decisions |
| [CAPABILITIES.md](CAPABILITIES.md) | Capabilities and use-case mapping |

## Protocol & feature coverage
| Doc | What's inside |
|---|---|
| [CONFORMANCE.md](CONFORMANCE.md) | Authoritative: 43 API keys, KIP coverage, Schema Registry conformance, gaps (vs Kafka 4.3) |
| [KIPS.md](KIPS.md) | KIP support matrix |

## Compatibility
| Doc | What's inside |
|---|---|
| [COMPATIBILITY.md](COMPATIBILITY.md) | Go + broker release matrix |
| [KAFKA_VERSIONS.md](KAFKA_VERSIONS.md) | Apache Kafka version notes (3.9–4.3) |
| [REDPANDA.md](REDPANDA.md) | Redpanda support (CI-verified), what's skipped, bugs it surfaced |

## Operate & tune
| Doc | What's inside |
|---|---|
| [PERFORMANCE.md](PERFORMANCE.md) | Tuning, benchmarks, best practices, anti-patterns |
| [TESTING.md](TESTING.md) | Test policy; unit/integration setup; `kfake` mock broker |

## Security & encoding details
| Doc | What's inside |
|---|---|
| [GSSAPI.md](GSSAPI.md) | GSSAPI / Kerberos SASL (SPNEGO pass-through) |
| [ZSTD.md](ZSTD.md) | zstd compression status |

## Project meta
| Doc | What's inside |
|---|---|
| [../CHANGELOG.md](../CHANGELOG.md) | Release history |
| [../CONTRIBUTING.md](../CONTRIBUTING.md) | How to contribute |
| [../SECURITY.md](../SECURITY.md) | Security policy & reporting |
| [LABELS.md](LABELS.md) | GitHub issue labels |

---

**Authoritative sources:** `README.md`, `CHANGELOG.md`, and `docs/CONFORMANCE.md` are the
source of truth; other docs and the `wiki/` vault synthesize and cross-link them.
