package limits

import "sync/atomic"

// Defaults guard against malicious broker/registry payloads (OOM / compression bombs).
const (
	DefaultMaxResponseBytes     = 64 << 20 // 64 MiB
	DefaultMaxDecompressedBytes = 64 << 20
	DefaultMaxSCRAMIterations   = 8192
	DefaultMaxHTTPBodyBytes     = 8 << 20
)

// Limits are process-global and read from hot paths (decode, decompress, auth,
// schema HTTP). They are stored atomically so concurrent NewClient calls and
// in-flight requests never race. Note: because they are process-global, the
// most recent NewClient wins if two clients configure different limits.
var (
	maxResponseBytes     atomic.Int64
	maxDecompressedBytes atomic.Int64
	maxSCRAMIterations   atomic.Int64
	maxHTTPBodyBytes     atomic.Int64
)

func init() {
	maxResponseBytes.Store(DefaultMaxResponseBytes)
	maxDecompressedBytes.Store(DefaultMaxDecompressedBytes)
	maxSCRAMIterations.Store(DefaultMaxSCRAMIterations)
	maxHTTPBodyBytes.Store(DefaultMaxHTTPBodyBytes)
}

// MaxResponseBytes is the cap on a single Kafka response frame.
func MaxResponseBytes() int { return int(maxResponseBytes.Load()) }

// MaxDecompressedBytes is the cap on a decompressed record batch.
func MaxDecompressedBytes() int { return int(maxDecompressedBytes.Load()) }

// MaxSCRAMIterations is the cap on a server-advertised SCRAM iteration count.
func MaxSCRAMIterations() int { return int(maxSCRAMIterations.Load()) }

// MaxHTTPBodyBytes is the cap on a Schema Registry HTTP response body.
func MaxHTTPBodyBytes() int { return int(maxHTTPBodyBytes.Load()) }

// Config holds optional client resource limits (zero = use defaults).
type Config struct {
	MaxResponseBytes     int
	MaxDecompressedBytes int
	MaxSCRAMIterations   int
	MaxHTTPBodyBytes     int
}

// Apply updates process limits from cfg (non-zero fields only).
func Apply(cfg Config) {
	if cfg.MaxResponseBytes > 0 {
		maxResponseBytes.Store(int64(cfg.MaxResponseBytes))
	}
	if cfg.MaxDecompressedBytes > 0 {
		maxDecompressedBytes.Store(int64(cfg.MaxDecompressedBytes))
	}
	if cfg.MaxSCRAMIterations > 0 {
		maxSCRAMIterations.Store(int64(cfg.MaxSCRAMIterations))
	}
	if cfg.MaxHTTPBodyBytes > 0 {
		maxHTTPBodyBytes.Store(int64(cfg.MaxHTTPBodyBytes))
	}
}
