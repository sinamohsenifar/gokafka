# ZSTD compression status

GoKafka is **stdlib-only** (`go.mod` has zero third-party dependencies). ZSTD (RFC 8878) is implemented in pure Go under `internal/compress/zstd/`.

## Kafka usage

- Record batch attribute `0x04` indicates ZSTD compression
- Brokers and most Java clients enable ZSTD by default in modern versions

## Current behavior

- `CompressionZstd` is supported for produce and fetch
- `internal/compress` encodes/decodes standard ZSTD frames (same as Java clients and brokers)
- Decompression respects `limits.MaxDecompressedBytes`
- Produce skips compression when compressed size is not smaller than uncompressed payload

## Implementation

- **Decoder** — adapted from the Go standard library `internal/zstd` decompressor (BSD license)
- **Encoder** — pure-Go block encoder with predefined FSE tables and greedy LZ matching
- Public API: `compress.ZstdEncode` / `compress.ZstdDecode` and `compress.Compress` / `compress.Decompress` with `CodecZstd`

## Workaround

If you prefer not to use ZSTD, **gzip**, **snappy**, and **lz4** remain available.
