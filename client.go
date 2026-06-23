package gokafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/sinamohsenifar/gokafka/internal/broker"
	"github.com/sinamohsenifar/gokafka/internal/limits"
	"github.com/sinamohsenifar/gokafka/metrics"
	"github.com/sinamohsenifar/gokafka/observe"
	"github.com/sinamohsenifar/gokafka/schema"
)

// Client is the root Kafka client (pure Go, stdlib only).
type Client struct {
	cfg          Config
	cluster      *broker.Cluster
	observe      *observe.Hub
	sharedProd   *Producer
	sharedProdMu sync.Mutex
	closed       bool
	mu           sync.Mutex
}

// NewClient connects to seed brokers and loads cluster metadata.
func NewClient(cfg Config) (*Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	limits.Apply(limits.Config{
		MaxResponseBytes:     cfg.Connection.Limits.MaxResponseBytes,
		MaxDecompressedBytes: cfg.Connection.Limits.MaxDecompressedBytes,
		MaxSCRAMIterations:   cfg.Connection.Limits.MaxSCRAMIterations,
		MaxHTTPBodyBytes:     cfg.Connection.Limits.MaxHTTPBodyBytes,
	})
	obsCfg := cfg.Observability.Config
	if obsCfg.Metrics.Namespace == "" {
		obsCfg.Metrics.Namespace = cfg.Metrics.Namespace
	}
	obsCfg.Metrics.Enabled = cfg.Metrics.Enabled
	if obsCfg.ServiceName == "" {
		obsCfg.ServiceName = cfg.ClientID
	}
	obsCfg.Version = Version
	hub := observe.NewHub(obsCfg)
	for _, hook := range cfg.Observability.extraHooks {
		hub.RegisterMetricsHook(hook)
	}
	c := &Client{
		cfg:     cfg,
		observe: hub,
		cluster: broker.New(cfg.Brokers, cfg.ClientID, cfg.Security, broker.Options{
			DialTimeout:       cfg.Connection.DialTimeout,
			RequestTimeout:    cfg.Connection.RequestTimeout,
			MetadataTTL:       cfg.Connection.MetadataTTL,
			MaxResponseBytes:  limits.MaxResponseBytes,
			HostRemap:         cfg.Connection.HostRemap,
			AddressMapper:     cfg.Connection.BrokerAddressMapper,
			Observe:           hub,
		}),
	}
	ctx := context.Background()
	_, span := hub.StartSpan(ctx, "gokafka.connect")
	if err := c.cluster.Refresh(ctx, nil); err != nil {
		span.RecordError(err)
		span.SetStatus(observe.StatusError, err.Error())
		span.End()
		c.cluster.Close()
		hub.Log(ctx, observe.LevelError, "metadata refresh failed", observe.Error(err))
		return nil, fmt.Errorf("gokafka: metadata: %w", err)
	}
	if err := c.cluster.NegotiateVersions(ctx, Version); err != nil {
		hub.Log(ctx, observe.LevelWarn, "api version negotiation failed, using defaults", observe.Error(err))
	}
	span.End()
	hub.Log(ctx, observe.LevelInfo, "client connected", observe.String("client.id", cfg.ClientID))
	return c, nil
}

// Close releases broker connections.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.cluster.Close()
	c.closed = true
	c.observe.Log(context.Background(), observe.LevelInfo, "client closed")
}

func (c *Client) requireOpen() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	return nil
}

// Ping refreshes cluster metadata.
func (c *Client) Ping(ctx context.Context) error {
	if err := c.requireOpen(); err != nil {
		return err
	}
	return c.cluster.Refresh(ctx, nil)
}

// Consumer returns a consumer group reader.
func (c *Client) Consumer(topics []string) *Consumer {
	return &Consumer{client: c, topics: append([]string(nil), topics...)}
}

// Admin returns cluster admin operations.
func (c *Client) Admin() *Admin {
	return &Admin{client: c}
}

// SchemaRegistry creates a Schema Registry client.
func (c *Client) SchemaRegistry(cfg SchemaRegistryConfig) (*schema.Registry, error) {
	if cfg.URL == "" {
		return nil, ErrNoSchemaURL
	}
	return schema.New(cfg)
}

// Metrics returns a point-in-time metrics snapshot.
func (c *Client) Metrics() metrics.Snapshot {
	return c.observe.Metrics.Snapshot()
}

// Producer returns the shared sync producer (single idempotent sequence state per client).
func (c *Client) Producer() *Producer {
	c.sharedProdMu.Lock()
	defer c.sharedProdMu.Unlock()
	if c.sharedProd == nil {
		c.sharedProd = &Producer{
			client:      c,
			partitioner: partitionerFromConfig(c.cfg.Producer),
		}
	}
	return c.sharedProd
}

// Version returns the module semver string.
func VersionString() string { return Version }
