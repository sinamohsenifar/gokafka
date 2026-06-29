package broker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/auth"
	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/internal/transport"
	"github.com/sinamohsenifar/gokafka/internal/wire"
	"github.com/sinamohsenifar/gokafka/observe"
)

// Options configures cluster networking behavior.
type Options struct {
	DialTimeout        time.Duration
	RequestTimeout     time.Duration
	MetadataTTL        time.Duration
	MaxResponseBytes   int
	HostRemap          map[string]string
	AddressMapper      func(nodeID int32, host string, port int32) string
	AllowedBrokerHosts []string
	Observe            *observe.Hub
}

func (o Options) resolveAddress(nodeID int32, host string, port int32) string {
	if o.AddressMapper != nil {
		if addr := o.AddressMapper(nodeID, host, port); addr != "" {
			return addr
		}
	}
	key := fmt.Sprintf("%s:%d", host, port)
	if o.HostRemap != nil {
		if mapped, ok := o.HostRemap[key]; ok {
			return mapped
		}
	}
	return key
}

func (o Options) brokerHostAllowed(host string) bool {
	if len(o.AllowedBrokerHosts) == 0 {
		return true
	}
	host = strings.ToLower(strings.TrimSpace(host))
	for _, allowed := range o.AllowedBrokerHosts {
		if host == strings.ToLower(strings.TrimSpace(allowed)) {
			return true
		}
	}
	return false
}

// Cluster maintains metadata and broker connections.
type Cluster struct {
	Seeds    []string
	ClientID string
	Security auth.Config
	Opts     Options

	mu               sync.RWMutex
	meta             protocol.MetadataResponse
	leaderIndex      map[string]map[int32]int32 // topic -> partition -> broker node id
	leaderEpochIndex map[string]map[int32]int32 // topic -> partition -> leader epoch (-1 unknown)
	metaRefreshedAt  time.Time
	conns            map[int32]*transport.Conn
	seedConn         *transport.Conn
	apiVersions      map[int16]int16
}

func New(seeds []string, clientID string, sec auth.Config, opts Options) *Cluster {
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = 10 * time.Second
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 30 * time.Second
	}
	return &Cluster{
		Seeds:    seeds,
		ClientID: clientID,
		Security: sec,
		Opts:     opts,
		conns:    map[int32]*transport.Conn{},
	}
}

func (c *Cluster) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	if c.seedConn != nil {
		_ = c.seedConn.Close()
	}
	c.conns = map[int32]*transport.Conn{}
}

func (c *Cluster) Invalidate(nodeID int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[nodeID]; ok {
		_ = conn.Close()
		delete(c.conns, nodeID)
	}
}

func (c *Cluster) Refresh(ctx context.Context, topics []string) error {
	if err := c.refresh(ctx, topics); err != nil {
		return err
	}
	c.mu.Lock()
	c.metaRefreshedAt = time.Now()
	c.mu.Unlock()
	return nil
}

// RefreshIfStale reloads metadata when the TTL expired or force is true.
func (c *Cluster) RefreshIfStale(ctx context.Context, topics []string, force bool) error {
	ttl := c.Opts.MetadataTTL
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	if !force {
		c.mu.RLock()
		fresh := !c.metaRefreshedAt.IsZero() && time.Since(c.metaRefreshedAt) < ttl
		c.mu.RUnlock()
		if fresh {
			return nil
		}
	}
	return c.Refresh(ctx, topics)
}

func (c *Cluster) refresh(ctx context.Context, topics []string) error {
	ver := c.negotiatedVersion(protocol.APIMetadata, protocol.VerMetadata)
	body := protocol.EncodeMetadataRequest(ver, topics)
	// If the cached seed broker has died, its connection fails here; drop it and
	// re-dial another seed so metadata refresh survives losing the bootstrap broker.
	var lastErr error
	for attempt := 0; attempt <= len(c.Seeds); attempt++ {
		conn, err := c.seed(ctx)
		if err != nil {
			return err
		}
		resp, err := conn.Request(ctx, protocol.APIMetadata, ver, body)
		if err != nil {
			c.dropSeed(conn)
			lastErr = err
			continue
		}
		respBody, err := transport.ResponseBodyForAPI(resp, protocol.APIMetadata, ver)
		if err != nil {
			c.dropSeed(conn)
			lastErr = err
			continue
		}
		meta, err := protocol.DecodeMetadataResponse(ver, respBody)
		if err != nil {
			return err
		}
		c.mu.Lock()
		c.meta = meta
		c.leaderIndex = buildLeaderIndex(meta)
		c.leaderEpochIndex = buildLeaderEpochIndex(meta)
		c.mu.Unlock()
		return nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("broker: metadata refresh failed")
	}
	return lastErr
}

// dropSeed closes and clears the cached seed connection so the next seed() call
// re-dials a (possibly different, live) broker.
func (c *Cluster) dropSeed(conn *transport.Conn) {
	c.mu.Lock()
	if c.seedConn == conn {
		c.seedConn = nil
	}
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func buildLeaderIndex(meta protocol.MetadataResponse) map[string]map[int32]int32 {
	out := make(map[string]map[int32]int32, len(meta.Topics))
	for _, t := range meta.Topics {
		parts := make(map[int32]int32, len(t.Partitions))
		for _, p := range t.Partitions {
			parts[p.Partition] = p.Leader
		}
		out[t.Name] = parts
	}
	return out
}

func buildLeaderEpochIndex(meta protocol.MetadataResponse) map[string]map[int32]int32 {
	out := make(map[string]map[int32]int32, len(meta.Topics))
	for _, t := range meta.Topics {
		parts := make(map[int32]int32, len(t.Partitions))
		for _, p := range t.Partitions {
			parts[p.Partition] = p.LeaderEpoch
		}
		out[t.Name] = parts
	}
	return out
}

// LeaderEpoch returns the current leader epoch for a partition, or -1 if unknown
// (used as Fetch current_leader_epoch for KIP-320 stale-leader fencing).
func (c *Cluster) LeaderEpoch(topic string, partition int32) int32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if parts, ok := c.leaderEpochIndex[topic]; ok {
		if e, ok := parts[partition]; ok {
			return e
		}
	}
	return -1
}

// LeaderNodeID returns the broker node id leading a topic partition.
func (c *Cluster) LeaderNodeID(topic string, partition int32) (int32, bool) {
	return c.partitionLeader(topic, partition)
}

func (c *Cluster) partitionLeader(topic string, part int32) (int32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	parts, ok := c.leaderIndex[topic]
	if !ok {
		return 0, false
	}
	leader, ok := parts[part]
	return leader, ok
}

func (c *Cluster) Metadata() protocol.MetadataResponse {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.meta
}

// TopicNameByID resolves a topic UUID from cached metadata (requires metadata v10+).
func (c *Cluster) TopicNameByID(id wire.UUID) (string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, t := range c.meta.Topics {
		if t.TopicID == id {
			return t.Name, true
		}
	}
	return "", false
}

// TopicIDByName returns the topic UUID from cached metadata.
func (c *Cluster) TopicIDByName(name string) (wire.UUID, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, t := range c.meta.Topics {
		if t.Name == name {
			return t.TopicID, true
		}
	}
	return wire.UUID{}, false
}

func (c *Cluster) LeaderBroker(topic string, partition int32) (protocol.Broker, error) {
	c.mu.RLock()
	leader, ok := c.leaderIndex[topic][partition]
	brokers := c.meta.Brokers
	c.mu.RUnlock()
	if !ok {
		return protocol.Broker{}, fmt.Errorf("broker: leader not found for %s-%d", topic, partition)
	}
	for _, b := range brokers {
		if b.NodeID == leader {
			return b, nil
		}
	}
	return protocol.Broker{}, fmt.Errorf("broker: leader not found for %s-%d", topic, partition)
}

func (c *Cluster) Conn(ctx context.Context, nodeID int32) (*transport.Conn, error) {
	c.refreshSASLToken(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[nodeID]; ok {
		return conn, nil
	}
	var broker protocol.Broker
	for _, b := range c.meta.Brokers {
		if b.NodeID == nodeID {
			broker = b
			break
		}
	}
	if broker.Host == "" {
		return nil, fmt.Errorf("broker: unknown node %d", nodeID)
	}
	if !c.Opts.brokerHostAllowed(broker.Host) {
		return nil, fmt.Errorf("broker: host %q not allowed", broker.Host)
	}
	addr := c.Opts.resolveAddress(broker.NodeID, broker.Host, broker.Port)
	conn, err := transport.Dial(ctx, addr, c.ClientID, c.Security, c.Opts.DialTimeout, c.Opts.RequestTimeout, c.Opts.MaxResponseBytes)
	if err != nil {
		return nil, err
	}
	c.conns[nodeID] = conn
	return conn, nil
}

func (c *Cluster) Request(ctx context.Context, nodeID int32, apiKey, apiVersion int16, body []byte) ([]byte, error) {
	if v := c.negotiatedVersion(apiKey, apiVersion); v > 0 && apiVersion > v {
		apiVersion = v
	}
	start := time.Now()
	conn, err := c.Conn(ctx, nodeID)
	if err != nil {
		c.recordRequest(apiKey, time.Since(start), err)
		return nil, err
	}
	resp, err := conn.Request(ctx, apiKey, apiVersion, body)
	if err != nil {
		if c.Security.SASLEnabled() && c.Security.SASL.TokenProvider != nil {
			if reauthErr := conn.Reauthenticate(ctx); reauthErr == nil {
				resp, err = conn.Request(ctx, apiKey, apiVersion, body)
			}
		}
	}
	if err != nil {
		c.Invalidate(nodeID)
		c.recordRequest(apiKey, time.Since(start), err)
		return nil, err
	}
	c.recordRequest(apiKey, time.Since(start), nil)
	return transport.ResponseBodyForAPI(resp, apiKey, apiVersion)
}

func (c *Cluster) seed(ctx context.Context) (*transport.Conn, error) {
	c.mu.RLock()
	if c.seedConn != nil {
		c.mu.RUnlock()
		return c.seedConn, nil
	}
	c.mu.RUnlock()
	var lastErr error
	for _, s := range c.Seeds {
		if host, _, ok := strings.Cut(s, ":"); ok && !c.Opts.brokerHostAllowed(host) {
			lastErr = fmt.Errorf("broker: seed host %q not allowed", host)
			continue
		}
		c.refreshSASLToken(ctx)
		conn, err := transport.Dial(ctx, s, c.ClientID, c.Security, c.Opts.DialTimeout, c.Opts.RequestTimeout, c.Opts.MaxResponseBytes)
		if err != nil {
			lastErr = err
			continue
		}
		c.mu.Lock()
		c.seedConn = conn
		c.mu.Unlock()
		return conn, nil
	}
	return nil, fmt.Errorf("broker: dial seeds: %w", lastErr)
}

func (c *Cluster) recordRequest(apiKey int16, d time.Duration, err error) {
	if c.Opts.Observe != nil && c.Opts.Observe.Metrics != nil {
		c.Opts.Observe.Metrics.OnRequest(apiKey, d, err)
	}
}

// FindCoordinator resolves the broker node id for a group or transactional id.
func (c *Cluster) FindCoordinator(ctx context.Context, key string, keyType int8) (int32, error) {
	body := protocol.EncodeFindCoordinatorRequest(key, keyType)
	wait := 100 * time.Millisecond
	maxWait := 2 * time.Second
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		resp, err := c.RequestViaSeed(ctx, protocol.APIFindCoordinator, protocol.VerFindCoordinator, body)
		if err != nil {
			return 0, err
		}
		coord, err := protocol.DecodeFindCoordinatorResponse(resp)
		if err != nil {
			return 0, err
		}
		if coord.ErrorCode == 0 {
			return coord.NodeID, nil
		}
		lastErr = fmt.Errorf("broker: find coordinator: error %d", coord.ErrorCode)
		if !protocol.CoordinatorRetriable(coord.ErrorCode) {
			return 0, lastErr
		}
		if attempt == 19 {
			break
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
		wait *= 2
		if wait > maxWait {
			wait = maxWait
		}
	}
	return 0, lastErr
}

// TransactionCoordinator resolves the transaction coordinator for a transactional id.
func (c *Cluster) TransactionCoordinator(ctx context.Context, txnID string) (int32, error) {
	return c.FindCoordinator(ctx, txnID, protocol.CoordinatorTransaction)
}

// NegotiateVersions queries broker API ranges and stores negotiated versions for subsequent requests.
func (c *Cluster) NegotiateVersions(ctx context.Context, softwareVersion string) error {
	body := protocol.EncodeApiVersionsRequest(c.ClientID, softwareVersion)
	resp, err := c.RequestViaSeed(ctx, protocol.APIApiVersions, protocol.VerApiVersions, body)
	if err != nil {
		return err
	}
	versions, code, err := protocol.DecodeApiVersionsResponse(protocol.VerApiVersions, resp)
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("broker: api versions: error %d", code)
	}
	negotiated := map[int16]int16{}
	for _, v := range versions {
		clientMax := protocol.ClientVersion(v.APIKey)
		if clientMax == 0 {
			continue
		}
		if ver := protocol.NegotiateVersion(versions, v.APIKey, clientMax); ver > 0 {
			negotiated[v.APIKey] = ver
		}
	}
	c.apiVerMuLock(negotiated)
	return nil
}

func (c *Cluster) apiVerMuLock(negotiated map[int16]int16) {
	c.mu.Lock()
	c.apiVersions = negotiated
	c.mu.Unlock()
}

func (c *Cluster) refreshSASLToken(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Security.SASL.TokenProvider == nil || c.Security.SASL.Mechanism != auth.SASLOAuth {
		return
	}
	if token, err := c.Security.SASL.TokenProvider(ctx); err == nil {
		c.Security.SASL.Token = token
	}
}

// SupportsAPI reports whether the broker negotiated a non-zero version for an API key.
func (c *Cluster) SupportsAPI(apiKey int16) bool {
	return c.NegotiatedVersion(apiKey, 0) > 0
}

func (c *Cluster) negotiatedVersion(apiKey, fallback int16) int16 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.apiVersions == nil {
		return fallback
	}
	if v, ok := c.apiVersions[apiKey]; ok {
		return v
	}
	return fallback
}

// NegotiatedVersion returns the negotiated API version for a key, or fallback if not negotiated.
func (c *Cluster) NegotiatedVersion(apiKey, fallback int16) int16 {
	return c.negotiatedVersion(apiKey, fallback)
}

// NegotiatedVersions returns a copy of negotiated API versions keyed by API id.
func (c *Cluster) NegotiatedVersions() map[int16]int16 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.apiVersions) == 0 {
		return nil
	}
	out := make(map[int16]int16, len(c.apiVersions))
	for k, v := range c.apiVersions {
		out[k] = v
	}
	return out
}

// RequestViaSeed sends a request through a seed broker connection.
func (c *Cluster) RequestViaSeed(ctx context.Context, apiKey, apiVersion int16, body []byte) ([]byte, error) {
	return c.requestSeed(ctx, apiKey, apiVersion, body, false)
}

// RequestAny tries metadata brokers in order, then falls back to seed brokers.
func (c *Cluster) RequestAny(ctx context.Context, apiKey, apiVersion int16, body []byte) ([]byte, error) {
	c.mu.RLock()
	brokers := append([]protocol.Broker(nil), c.meta.Brokers...)
	c.mu.RUnlock()

	var lastErr error
	for _, b := range brokers {
		resp, err := c.Request(ctx, b.NodeID, apiKey, apiVersion, body)
		if err == nil {
			return resp, nil
		}
		lastErr = err
	}
	resp, err := c.requestSeed(ctx, apiKey, apiVersion, body, true)
	if err == nil {
		return resp, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, err
}

func (c *Cluster) requestSeed(ctx context.Context, apiKey, apiVersion int16, body []byte, retry bool) ([]byte, error) {
	if v := c.negotiatedVersion(apiKey, apiVersion); v > 0 && apiVersion > v {
		apiVersion = v
	}
	start := time.Now()
	conn, err := c.seed(ctx)
	if err != nil {
		c.recordRequest(apiKey, time.Since(start), err)
		return nil, err
	}
	resp, err := conn.Request(ctx, apiKey, apiVersion, body)
	if err != nil {
		c.invalidateSeed()
		if retry {
			if conn2, err2 := c.seed(ctx); err2 == nil {
				resp, err = conn2.Request(ctx, apiKey, apiVersion, body)
				if err != nil {
					c.invalidateSeed()
				}
			}
		}
	}
	c.recordRequest(apiKey, time.Since(start), err)
	if err != nil {
		return nil, err
	}
	return transport.ResponseBodyForAPI(resp, apiKey, apiVersion)
}

func (c *Cluster) invalidateSeed() {
	c.mu.Lock()
	if c.seedConn != nil {
		_ = c.seedConn.Close()
		c.seedConn = nil
	}
	c.mu.Unlock()
}
