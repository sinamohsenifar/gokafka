# Architecture

How GoKafka is built. This is the developer/contributor view; for the user-facing
API see the [README](../README.md), and for protocol coverage see
[CONFORMANCE.md](CONFORMANCE.md).

GoKafka is a layered, **pure-Go (stdlib-only, no CGO)** Kafka client. A small public
API sits over internal subsystems that speak the Kafka wire protocol.

```
gokafka (public)              Client · Producer · Consumer · ShareConsumer · Admin
  ├─ schema/                  Schema Registry client, Serde, MockRegistry, CSFLE
  ├─ kfake/                   in-process mock broker (test-only)
  ├─ observe/, metrics/       pluggable logging / tracing / metrics
  └─ internal/
       ├─ broker/  (Cluster)  metadata, leader & coordinator resolution, version negotiation
       ├─ transport/ (Conn)   TCP framing, request/response, SASL/TLS dial
       ├─ protocol/           per-API encode/decode, API keys, KIP logic
       ├─ wire/   (Buffer)    primitive read/write, compact/varint, tagged fields
       ├─ produce/, compress/ record batching, codecs
       └─ auth/               SASL (PLAIN/SCRAM/OAUTHBEARER/GSSAPI), TLS config
```

## Request path

1. **Connect** — `gokafka.NewClient` builds the `Cluster`, then:
   - `Cluster.NegotiateVersions` sends **ApiVersions v3** and records the negotiated
     version per API (see [Version negotiation](#version-negotiation)) plus any
     cluster-finalized features (e.g. `transaction.version`).
   - `Cluster.Refresh` fetches **Metadata**. On failure a TLS-mismatch hint is added
     if a plaintext client hit a TLS-only broker (an opaque EOF otherwise).
2. **Build** — a public call (e.g. `Producer.ProduceSync`) encodes a request body via
   `internal/protocol` at the **negotiated** version.
3. **Route** — `Cluster` sends it to the right broker: the partition leader for
   produce/fetch, the group/transaction coordinator for group ops, the controller
   (forwarded) for most admin ops.
4. **Frame** — `internal/transport` writes the length-prefixed frame and reads the
   response; `ResponseBodyForAPI` strips the response header (and, for flexible APIs,
   its tag section — *except* ApiVersions, per KIP-511).
5. **Decode** — `internal/protocol` parses the response body.

## Flexible vs legacy encoding

Each API version is either **legacy** (int16/int32 lengths) or **flexible** (KIP-482:
compact strings/arrays via `uvarint`, plus a trailing **tag section** on every struct
and on the request/response). `internal/wire.Buffer` provides both sets of primitives;
`internal/protocol/flex.go` decides per API+version via `flexibleRequestHeader` /
`flexibleResponseHeader`.

> **KIP-511 exception:** the ApiVersions *response header* is always non-flexible, even
> at v3+, so a client can parse it before it knows the broker's capabilities.

### The decode-bug pattern ⚠️

A recurring bug class: a flexible-struct decoder **omits a field** whose value is often
a single `0x00` byte (a null compact string / empty compact array). The next
`SkipTagSection()` absorbs that `0x00` as "0 tags", so the decoder stays aligned **as
long as the field is null** — and desyncs ("buffer too short") the moment a broker
returns a non-null value. This stayed latent against Apache Kafka and was surfaced by
testing against [Redpanda](REDPANDA.md) (DescribeConfigs synonyms, CreatePartitions
`error_message`). **Lesson: test against a second Kafka-compatible broker.**

## Version negotiation

At connect, for each API the broker advertises, `Cluster` stores
`min(clientCeiling, brokerMax)`, so the client adapts to any broker (Kafka 3.9–4.3,
[Redpanda](REDPANDA.md)). Subtleties learned the hard way:

- **v0-max APIs must be recorded.** An API a broker advertises with max version 0
  negotiates to v0 — that must override the client's higher default, or the broker
  resets the connection. `ClientVersion` returns `-1` for unimplemented APIs to
  distinguish "v0 supported" from "unknown".
- **`AdvertisesAPI`** lets `Admin` return a clear *"broker does not support API key N"*
  error for unadvertised APIs instead of an opaque EOF.
- A few APIs are **pinned** (e.g. OffsetFetch single v7 for `require_stable`); most are
  negotiated down.

## Subsystems

| Package | Responsibility |
|---|---|
| `internal/broker` (`Cluster`) | metadata cache, leader/coordinator resolution, leader-epoch index, version negotiation, routing (`Request`, `RequestAny`, `RequestViaSeed`) |
| `internal/transport` (`Conn`) | one in-flight RPC per TCP conn; request framing; SASL/TLS dial; OAuth re-auth (KIP-368) |
| `internal/protocol` | request encoders + response decoders for every API; `flex.go`, `keys.go`, `versions.go` |
| `internal/wire` (`Buffer`) | byte primitives: int/compact/varint/uuid, tag sections, bounds checks |
| `internal/produce` | v2 record-batch building; sequence numbers |
| `internal/compress` | gzip/snappy/lz4/zstd, all pure Go |
| `internal/auth` | SASL mechanisms + TLS config |
| `schema/` | Schema Registry client, Avro/JSON/Protobuf serdes, `MockRegistry`, CSFLE |
| `kfake/` | in-process mock broker for tests (the real client is its correctness oracle) |
| `observe/`, `metrics/` | pluggable Logger/Tracer/MetricsRecorder; Prometheus/OTel/slog bridges |

## Testing strategy

- **Unit** (`-race`) for codecs and pure logic.
- **Integration** (`-tags=integration`) against real brokers in CI: the Kafka matrix
  (3.9.2–4.3.0 × Go 1.22–1.26) and a [Redpanda](REDPANDA.md) lane.
- **kfake** lets *users* test their producer/consumer/admin code against the real client
  with no Docker. See [TESTING.md](TESTING.md).

## Design decisions

- **Stdlib-only, no CGO** — single static binary; full control over the wire protocol
  (which is how the decode bugs were found and fixed). Trade-off: KIP-714 client-metrics
  push is a non-goal (OTLP/protobuf), and CSFLE leaves cloud-KMS drivers to the caller.
- **Real client as the mock-broker oracle** — kfake advertises narrow version ranges so
  the client negotiates down to what it implements; if kfake's encoding were wrong, the
  client's own decoders would reject it.
- **Incremental KIP-890 TV2** — landed in safe, independently-shippable steps; the first
  attempt was caught by integration tests before shipping.

See also the navigable Obsidian knowledge map under [`wiki/`](../wiki/index.md).
