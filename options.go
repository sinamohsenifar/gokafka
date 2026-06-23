package gokafka

import (
	"runtime"
	"time"
)

// Option configures a [Config] using functional options (recommended pattern).
type Option func(*Config)

// WithClientID sets the Kafka client.id.
func WithClientID(id string) Option {
	return func(c *Config) { c.ClientID = id }
}

// WithConsumerGroup sets the consumer group id.
func WithConsumerGroup(group string) Option {
	return func(c *Config) { c.ConsumerGroup = group }
}

// WithSecurity sets connection security (TLS + SASL).
func WithSecurity(sec SecurityConfig) Option {
	return func(c *Config) { c.Security = sec }
}

// WithProducer merges producer settings into the client config.
func WithProducer(ps ProducerConfig) Option {
	return func(c *Config) {
		if ps.Acks != 0 || ps.Acks == AcksNone {
			c.Producer.Acks = ps.Acks
		}
		if ps.Compression != 0 {
			c.Producer.Compression = ps.Compression
		}
		if ps.BatchSize > 0 {
			c.Producer.BatchSize = ps.BatchSize
		}
		if ps.Linger > 0 {
			c.Producer.Linger = ps.Linger
		}
		if ps.Idempotent {
			c.Producer.Idempotent = true
		}
		if ps.PartitionStrategy != 0 {
			c.Producer.PartitionStrategy = ps.PartitionStrategy
		}
	}
}

// WithConsumer merges consumer settings into the client config without resetting unrelated fields.
func WithConsumer(cs ConsumerConfig) Option {
	return func(c *Config) {
		if cs.SessionTimeout > 0 {
			c.Consumer.SessionTimeout = cs.SessionTimeout
		}
		if cs.RebalanceTimeout > 0 {
			c.Consumer.RebalanceTimeout = cs.RebalanceTimeout
		}
		if cs.HeartbeatInterval > 0 {
			c.Consumer.HeartbeatInterval = cs.HeartbeatInterval
		}
		if cs.MaxPollRecords > 0 {
			c.Consumer.MaxPollRecords = cs.MaxPollRecords
		}
		if cs.Assignor != 0 {
			c.Consumer.Assignor = cs.Assignor
		}
		if cs.GroupProtocol != 0 {
			c.Consumer.GroupProtocol = cs.GroupProtocol
		}
		if cs.IsolationLevel != 0 {
			c.Consumer.IsolationLevel = cs.IsolationLevel
		}
		if cs.GroupInstanceID != "" {
			c.Consumer.GroupInstanceID = cs.GroupInstanceID
		}
	}
}

// WithAutoCommit enables automatic offset commits in Consumer.Run after successful handlers.
func WithAutoCommit(enabled bool) Option {
	return func(c *Config) { c.Consumer.AutoCommit = enabled }
}

// WithGroupProtocol selects classic or KIP-848 next-gen consumer group protocol.
func WithGroupProtocol(p GroupProtocol) Option {
	return func(c *Config) { c.Consumer.GroupProtocol = p }
}

// WithGroupInstanceID sets static group membership id (group.instance.id).
func WithGroupInstanceID(id string) Option {
	return func(c *Config) { c.Consumer.GroupInstanceID = id }
}

// WithConsumeFromBeginning seeks to earliest offset when no committed offset exists.
func WithConsumeFromBeginning(enabled bool) Option {
	return func(c *Config) { c.Consumer.ConsumeFromBeginning = enabled }
}

// WithTransaction configures exactly-once transactional producer settings.
func WithTransaction(ts TransactionConfig) Option {
	return func(c *Config) { c.Transaction = ts }
}

// WithConcurrency sets worker pool sizes for async APIs.
func WithConcurrency(n ConcurrencyConfig) Option {
	return func(c *Config) { c.Concurrency = n }
}

// WithConnection sets dial/request timeouts and advertised-listener host remapping.
func WithConnection(conn ConnectionConfig) Option {
	return func(c *Config) { c.Connection = conn }
}

// WithBrokerHostRemap maps unreachable advertised listener addresses to reachable ones.
func WithBrokerHostRemap(remap map[string]string) Option {
	return func(c *Config) {
		if c.Connection.HostRemap == nil {
			c.Connection.HostRemap = map[string]string{}
		}
		for k, v := range remap {
			c.Connection.HostRemap[k] = v
		}
	}
}

// WithMetrics enables in-process metrics collection and optional namespace.
func WithMetrics(enabled bool, namespace string) Option {
	return func(c *Config) {
		c.Metrics.Enabled = enabled
		c.Observability.Metrics.Enabled = enabled
		if namespace != "" {
			c.Metrics.Namespace = namespace
			c.Observability.Metrics.Namespace = namespace
		}
	}
}

// NewConfig builds a validated config from brokers and options.
func NewConfig(brokers []string, opts ...Option) (Config, error) {
	cfg := DefaultConfig(brokers)
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.Concurrency.ProducerWorkers <= 0 || cfg.Concurrency.ConsumerWorkers <= 0 {
		cfg.Concurrency = defaultConcurrency()
	}
	if cfg.Retry.MaxAttempts <= 0 {
		cfg.Retry = defaultRetry()
	}
	return cfg, cfg.validate()
}

// ProducerConfig controls produce durability and batching.
type ProducerConfig struct {
	Acks              Acks
	Compression       CompressionCodec
	Idempotent        bool
	TransactionalID   string
	BatchSize         int
	Linger            time.Duration
	PartitionStrategy ProducerPartitionStrategy
}

// ProducerPartitionStrategy selects how records are routed to partitions.
type ProducerPartitionStrategy int

const (
	ProducerPartitionHash ProducerPartitionStrategy = iota
	ProducerPartitionRoundRobin
)

// GroupProtocol selects classic JoinGroup/SyncGroup or KIP-848 ConsumerGroupHeartbeat.
type GroupProtocol int

const (
	GroupProtocolClassic GroupProtocol = iota
	GroupProtocolNextGen                 // KIP-848 experimental
)

// ConsumerConfig controls group consumption.
type ConsumerConfig struct {
	AutoCommit           bool
	ConsumeFromBeginning bool
	SessionTimeout       time.Duration
	RebalanceTimeout     time.Duration
	HeartbeatInterval    time.Duration
	MaxPollRecords       int
	Assignor             PartitionAssignor
	GroupProtocol        GroupProtocol
	IsolationLevel       IsolationLevel
	GroupInstanceID      string
}

// PartitionAssignor selects the consumer group partition assignment strategy.
type PartitionAssignor int

const (
	AssignorRange PartitionAssignor = iota
	AssignorRoundRobin
	AssignorSticky
	AssignorCooperativeSticky
)

func (a PartitionAssignor) protocolName() string {
	switch a {
	case AssignorRoundRobin:
		return "roundrobin"
	case AssignorSticky:
		return "sticky"
	case AssignorCooperativeSticky:
		return "cooperative-sticky"
	default:
		return "range"
	}
}

// TransactionConfig configures Kafka transactions (EOS).
type TransactionConfig struct {
	Enabled           bool
	TransactionalID   string
	Timeout           time.Duration
	IsolationLevel    IsolationLevel
}

// IsolationLevel for consumers reading transactional topics.
type IsolationLevel int

const (
	IsolationReadUncommitted IsolationLevel = iota
	IsolationReadCommitted
)

// ConcurrencyConfig controls async worker pools.
type ConcurrencyConfig struct {
	ProducerWorkers int
	ConsumerWorkers int
	ChannelBuffer   int
}

// RetryConfig controls automatic retries on retriable errors.
type RetryConfig struct {
	MaxAttempts int
	Backoff     time.Duration
	MaxBackoff  time.Duration
}

// Acks controls producer acknowledgement level.
type Acks int8

const (
	AcksAll  Acks = -1
	AcksOne  Acks = 1
	AcksNone Acks = 0
)

// PartitionStrategy for consumer group assignment preference (deprecated: use Assignor).
type PartitionStrategy string

const (
	PartitionStrategyRange PartitionStrategy = "range"
)

// WithRetry sets client retry policy.
func WithRetry(r RetryConfig) Option {
	return func(c *Config) { c.Retry = r }
}

func defaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		Acks:              AcksAll,
		Compression:       CompressionNone,
		Idempotent:        true,
		BatchSize:         100,
		Linger:            5 * time.Millisecond,
		PartitionStrategy: ProducerPartitionHash,
	}
}

func partitionerFromConfig(cfg ProducerConfig) Partitioner {
	switch cfg.PartitionStrategy {
	case ProducerPartitionRoundRobin:
		return &RoundRobinPartitioner{}
	default:
		return HashPartitioner{}
	}
}

func defaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		AutoCommit:        false,
		SessionTimeout:    45 * time.Second,
		HeartbeatInterval: 3 * time.Second,
		MaxPollRecords:    500,
		Assignor:          AssignorRange,
		IsolationLevel:    IsolationReadUncommitted,
	}
}

func defaultConcurrency() ConcurrencyConfig {
	n := runtime.NumCPU()
	if n < 2 {
		n = 2
	}
	return ConcurrencyConfig{
		ProducerWorkers: n,
		ConsumerWorkers: n,
		ChannelBuffer:   256,
	}
}

func defaultRetry() RetryConfig {
	return RetryConfig{MaxAttempts: 3, Backoff: 100 * time.Millisecond, MaxBackoff: 2 * time.Second}
}

// Security helpers for common setups.
func SCRAMSecurity(user, pass string, tls TLSConfig) SecurityConfig {
	return SecurityConfig{
		Protocol: SecuritySASLSSL,
		TLS:      tls,
		SASL: SASLConfig{
			Mechanism: SASLSCRAM512,
			Username:  user,
			Password:  pass,
		},
	}
}

func PlainSecurity(user, pass string, tls TLSConfig) SecurityConfig {
	return SecurityConfig{
		Protocol: SecuritySASLSSL,
		TLS:      tls,
		SASL: SASLConfig{
			Mechanism: SASLPlain,
			Username:  user,
			Password:  pass,
		},
	}
}

// SCRAMPlaintextSecurity configures SASL/SCRAM over SASL_PLAINTEXT (no TLS).
func SCRAMPlaintextSecurity(user, pass string, mech SASLMechanism) SecurityConfig {
	return SecurityConfig{
		Protocol: SecuritySASLPlaintext,
		SASL: SASLConfig{
			Mechanism: mech,
			Username:  user,
			Password:  pass,
		},
	}
}

// TLSOnlySecurity configures SSL/TLS without SASL.
func TLSOnlySecurity(tls TLSConfig) SecurityConfig {
	return SecurityConfig{Protocol: SecuritySSL, TLS: tls}
}

// OAuthBearerSecurity configures SASL/OAUTHBEARER over SASL_SSL.
func OAuthBearerSecurity(token string, tls TLSConfig) SecurityConfig {
	return SecurityConfig{
		Protocol: SecuritySASLSSL,
		TLS:      tls,
		SASL: SASLConfig{
			Mechanism: SASLOAuth,
			Token:     token,
		},
	}
}

// OAuthBearerPlaintextSecurity configures SASL/OAUTHBEARER over SASL_PLAINTEXT (dev only).
func OAuthBearerPlaintextSecurity(token string) SecurityConfig {
	return SecurityConfig{
		Protocol: SecuritySASLPlaintext,
		SASL: SASLConfig{
			Mechanism: SASLOAuth,
			Token:     token,
		},
	}
}
