package limits

// Defaults guard against malicious broker/registry payloads (OOM / compression bombs).
const (
	DefaultMaxResponseBytes     = 64 << 20 // 64 MiB
	DefaultMaxDecompressedBytes = 64 << 20
	DefaultMaxSCRAMIterations   = 8192
	DefaultMaxHTTPBodyBytes     = 8 << 20
)

var (
	MaxResponseBytes     = DefaultMaxResponseBytes
	MaxDecompressedBytes = DefaultMaxDecompressedBytes
	MaxSCRAMIterations   = DefaultMaxSCRAMIterations
	MaxHTTPBodyBytes     = DefaultMaxHTTPBodyBytes
)

// Config holds optional client resource limits (zero = use defaults).
type Config struct {
	MaxResponseBytes     int
	MaxDecompressedBytes int
	MaxSCRAMIterations   int
	MaxHTTPBodyBytes     int
}

// Apply updates package defaults from cfg (non-zero fields only).
func Apply(cfg Config) {
	if cfg.MaxResponseBytes > 0 {
		MaxResponseBytes = cfg.MaxResponseBytes
	}
	if cfg.MaxDecompressedBytes > 0 {
		MaxDecompressedBytes = cfg.MaxDecompressedBytes
	}
	if cfg.MaxSCRAMIterations > 0 {
		MaxSCRAMIterations = cfg.MaxSCRAMIterations
	}
	if cfg.MaxHTTPBodyBytes > 0 {
		MaxHTTPBodyBytes = cfg.MaxHTTPBodyBytes
	}
}
