package gokafka

import (
	"fmt"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/auth"
	"github.com/sinamohsenifar/gokafka/internal/compress"
	"github.com/sinamohsenifar/gokafka/metrics"
	"github.com/sinamohsenifar/gokafka/observe"
)

// Config is the root client configuration.
type Config struct {
	Brokers       []string
	ClientID      string
	ConsumerGroup string

	Connection     ConnectionConfig
	Security       auth.Config
	Metrics        metrics.Config
	Observability  observabilitySettings
	Producer       ProducerConfig
	Consumer       ConsumerConfig
	Transaction    TransactionConfig
	Concurrency    ConcurrencyConfig
	Retry          RetryConfig
}

type observabilitySettings struct {
	observe.Config
	extraHooks []observe.MetricsRecorder
}

// CompressionCodec is a record-batch compression type.
type CompressionCodec int

const (
	CompressionNone CompressionCodec = iota
	CompressionGzip
	CompressionSnappy
	CompressionLZ4
	CompressionZstd
)

// DefaultConfig returns defaults for a new client.
func DefaultConfig(brokers []string) Config {
	return Config{
		Brokers:     brokers,
		ClientID:    "gokafka",
		Connection:  defaultConnection(),
		Security:    auth.Config{Protocol: auth.SecurityPlaintext},
		Metrics:     metrics.Config{Namespace: "gokafka", Enabled: true},
		Observability: observabilitySettings{
			Config: observe.Config{
				ServiceName: "gokafka",
				LogLevel:    observe.LevelInfo,
				LogFormat:   observe.LogFormatJSON,
				Metrics:     observe.MetricsConfig{Namespace: "gokafka", Enabled: true},
			},
		},
		Producer:    defaultProducerConfig(),
		Consumer:    defaultConsumerConfig(),
		Concurrency: defaultConcurrency(),
		Retry:       defaultRetry(),
	}
}

func (c Config) validate() error {
	if len(c.Brokers) == 0 {
		return ErrNoBrokers
	}
	if c.Transaction.Enabled && c.transactionalID() == "" {
		return ErrNoTransactionalID
	}
	if c.Producer.Idempotent && c.Producer.Acks != AcksAll {
		return ErrInvalidProducerConfig
	}
	if c.Security.SASLEnabled() && c.Security.SASL.Mechanism == auth.SASLOAuth {
		if c.Security.SASL.Token == "" && c.Security.SASL.TokenProvider == nil {
			return fmt.Errorf("gokafka: OAUTHBEARER requires Token or TokenProvider")
		}
	}
	if c.Consumer.SessionTimeout > 0 && c.Consumer.HeartbeatInterval > 0 {
		if c.Consumer.HeartbeatInterval >= c.Consumer.SessionTimeout {
			return fmt.Errorf("gokafka: HeartbeatInterval must be less than SessionTimeout")
		}
	}
	return nil
}

func (c Config) compressionByte() int8 {
	switch c.Producer.Compression {
	case CompressionGzip:
		return compress.CodecGzip
	case CompressionSnappy:
		return compress.CodecSnappy
	case CompressionLZ4:
		return compress.CodecLZ4
	case CompressionZstd:
		return compress.CodecZstd
	default:
		return compress.CodecNone
	}
}

func (c Config) requestTimeout() time.Duration {
	if c.Connection.RequestTimeout > 0 {
		return c.Connection.RequestTimeout
	}
	return 30 * time.Second
}

func (c Config) transactionalID() string {
	if c.Producer.TransactionalID != "" {
		return c.Producer.TransactionalID
	}
	return c.Transaction.TransactionalID
}

func (c Config) transactionTimeoutMs() int32 {
	if c.Transaction.Timeout > 0 {
		ms := int32(c.Transaction.Timeout / time.Millisecond)
		if ms > 0 {
			return ms
		}
	}
	return 60000
}
