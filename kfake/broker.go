// Package kfake is an in-process mock Kafka broker for testing GoKafka-based
// code without a real cluster or Docker. It speaks enough of the Kafka wire
// protocol — at the exact API versions the GoKafka client negotiates — to
// support connect, admin (create/delete topics), produce, fetch, list-offsets,
// and single-member consumer groups (find-coordinator, join/sync/heartbeat/
// leave, offset commit/fetch).
//
// The real GoKafka client is the correctness oracle: every handler is exercised
// by running the actual client against the broker.
//
//	b, _ := kfake.NewBroker()
//	defer b.Close()
//	cfg, _ := gokafka.NewConfig([]string{b.Addr()})
//	client, _ := gokafka.NewClient(cfg)
//
// kfake is for tests; it is single-node, in-memory, and not durable.
package kfake

import (
	"encoding/binary"
	"io"
	"net"
	"sync"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// Kafka API keys handled by the mock.
const (
	apiProduce         = 0
	apiFetch           = 1
	apiListOffsets     = 2
	apiMetadata        = 3
	apiOffsetCommit    = 8
	apiOffsetFetch     = 9
	apiFindCoordinator = 10
	apiJoinGroup       = 11
	apiHeartbeat       = 12
	apiLeaveGroup      = 13
	apiSyncGroup       = 14
	apiApiVersions     = 18
	apiCreateTopics    = 19
	apiDeleteTopics    = 20
	apiInitProducerID  = 22
)

// Broker is an in-process mock Kafka broker.
type Broker struct {
	ln     net.Listener
	store  *store
	nodeID int32
	host   string
	port   int32
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool

	// Fault injection for tests: the next offsetFetchFailN single-group
	// OffsetFetch responses stamp offsetFetchFailCode as the per-partition
	// error, letting tests exercise the client's retry/completeness handling.
	offsetFetchFailN    int
	offsetFetchFailCode int16

	// The next produceFailN Produce responses stamp produceFailCode on every
	// partition and skip the log append, letting tests exercise the producer's
	// retry / partition-freeze behaviour without committing the faulted batch.
	produceFailN    int
	produceFailCode int16
}

// FailNextProduce makes the next n Produce responses return the given
// per-partition error code on every partition and NOT append the batch, then
// resume normal behaviour. Used to test that a produce retry reuses the same
// (frozen) partition assignment instead of re-running the partitioner.
func (b *Broker) FailNextProduce(n int, code int16) {
	b.mu.Lock()
	b.produceFailN = n
	b.produceFailCode = code
	b.mu.Unlock()
}

// takeProduceFault returns the error code to inject for this Produce response
// (0 = none) and decrements the remaining fault count.
func (b *Broker) takeProduceFault() int16 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.produceFailN <= 0 {
		return 0
	}
	b.produceFailN--
	return b.produceFailCode
}

// FailNextOffsetFetch makes the next n single-group (v7) OffsetFetch responses
// return the given per-partition error code on every partition, then resume
// normal behaviour. Used to test that the consumer retries a transient
// OffsetFetch (e.g. UNSTABLE_OFFSET_COMMIT / coordinator load) instead of
// silently leaving assigned partitions at offset 0.
func (b *Broker) FailNextOffsetFetch(n int, code int16) {
	b.mu.Lock()
	b.offsetFetchFailN = n
	b.offsetFetchFailCode = code
	b.mu.Unlock()
}

// takeOffsetFetchFault returns the error code to inject for this OffsetFetch
// response (0 = none) and decrements the remaining fault count.
func (b *Broker) takeOffsetFetchFault() int16 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.offsetFetchFailN <= 0 {
		return 0
	}
	b.offsetFetchFailN--
	return b.offsetFetchFailCode
}

// NewBroker starts a mock broker on a loopback port and returns it. Call Close
// when done.
func NewBroker() (*Broker, error) {
	return NewBrokerAt("127.0.0.1:0")
}

// NewBrokerAt starts a mock broker bound to a specific host:port. Use it to test
// client rebootstrap: bring up a replacement broker at the same bootstrap
// address a closed broker used (a port freed by a prior Close may briefly linger,
// so retry the bind).
func NewBrokerAt(addr string) (*Broker, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	host, port := splitHostPort(ln.Addr())
	b := &Broker{
		ln:     ln,
		store:  newStore(),
		nodeID: 0,
		host:   host,
		port:   port,
		conns:  map[net.Conn]struct{}{},
	}
	go b.acceptLoop()
	return b, nil
}

// Addr returns the broker's host:port for use as a bootstrap server.
func (b *Broker) Addr() string { return b.ln.Addr().String() }

// AddTopic pre-creates a topic with the given number of partitions, for test
// setup without an admin round-trip.
func (b *Broker) AddTopic(name string, partitions int32) {
	b.store.mu.Lock()
	b.store.createTopic(name, partitions)
	b.store.mu.Unlock()
}

// Close shuts the broker down and drops all connections.
func (b *Broker) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	_ = b.ln.Close()
	for c := range b.conns {
		_ = c.Close()
	}
	b.mu.Unlock()
}

func (b *Broker) acceptLoop() {
	for {
		conn, err := b.ln.Accept()
		if err != nil {
			return // listener closed
		}
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			_ = conn.Close()
			return
		}
		b.conns[conn] = struct{}{}
		b.mu.Unlock()
		go b.serve(conn)
	}
}

func (b *Broker) serve(conn net.Conn) {
	defer func() {
		b.mu.Lock()
		delete(b.conns, conn)
		b.mu.Unlock()
		_ = conn.Close()
	}()
	sizeBuf := make([]byte, 4)
	for {
		if _, err := io.ReadFull(conn, sizeBuf); err != nil {
			return
		}
		size := int(binary.BigEndian.Uint32(sizeBuf))
		if size <= 0 || size > 100<<20 {
			return
		}
		frame := make([]byte, size)
		if _, err := io.ReadFull(conn, frame); err != nil {
			return
		}
		resp, err := b.handle(frame)
		if err != nil {
			return
		}
		out := make([]byte, 4+len(resp))
		binary.BigEndian.PutUint32(out, uint32(len(resp)))
		copy(out[4:], resp)
		if _, err := conn.Write(out); err != nil {
			return
		}
	}
}

// handle parses a request frame and returns the response frame (after the
// length prefix): correlation id, optional response-header tag section, body.
func (b *Broker) handle(frame []byte) ([]byte, error) {
	buf := wire.FromBytes(frame)
	apiKey, err := buf.ReadInt16()
	if err != nil {
		return nil, err
	}
	apiVer, err := buf.ReadInt16()
	if err != nil {
		return nil, err
	}
	corrID, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	if _, err := buf.ReadString(); err != nil { // client_id (legacy string even in flex header)
		return nil, err
	}
	if flexRequestHeader(int(apiKey), int(apiVer)) {
		if err := skipTags(buf); err != nil {
			return nil, err
		}
	}
	body := buf.Remaining()

	respBody, err := b.dispatch(int(apiKey), int(apiVer), body)
	if err != nil {
		return nil, err
	}

	out := wire.NewBuffer(len(respBody) + 8)
	out.WriteInt32(corrID)
	if flexResponseHeader(int(apiKey), int(apiVer)) {
		out.WriteEmptyTagSection()
	}
	out.B = append(out.B, respBody...)
	return out.Bytes(), nil
}

func (b *Broker) dispatch(apiKey, apiVer int, body []byte) ([]byte, error) {
	switch apiKey {
	case apiApiVersions:
		return b.handleApiVersions()
	case apiMetadata:
		return b.handleMetadata(apiVer, body)
	case apiCreateTopics:
		return b.handleCreateTopics(apiVer, body)
	case apiDeleteTopics:
		return b.handleDeleteTopics(apiVer, body)
	case apiProduce:
		return b.handleProduce(apiVer, body)
	case apiListOffsets:
		return b.handleListOffsets(apiVer, body)
	case apiFetch:
		return b.handleFetch(apiVer, body)
	case apiFindCoordinator:
		return b.handleFindCoordinator(apiVer, body)
	case apiJoinGroup:
		return b.handleJoinGroup(apiVer, body)
	case apiSyncGroup:
		return b.handleSyncGroup(apiVer, body)
	case apiHeartbeat:
		return b.handleHeartbeat(apiVer, body)
	case apiLeaveGroup:
		return b.handleLeaveGroup(apiVer, body)
	case apiOffsetCommit:
		return b.handleOffsetCommit(apiVer, body)
	case apiOffsetFetch:
		return b.handleOffsetFetch(apiVer, body)
	case apiInitProducerID:
		return b.handleInitProducerID(apiVer, body)
	default:
		// Unknown API: respond with an empty body. The client will surface an
		// error if it needed this call, which keeps the mock honest about scope.
		return []byte{}, nil
	}
}

// flexRequestHeader reports whether a request header carries a tag section, at
// the versions the GoKafka client uses.
func flexRequestHeader(apiKey, apiVer int) bool {
	switch apiKey {
	case apiApiVersions:
		return apiVer >= 3
	case apiMetadata, apiProduce:
		return apiVer >= 9
	case apiFetch:
		return apiVer >= 12
	case apiListOffsets, apiOffsetFetch:
		return apiVer >= 6
	case apiFindCoordinator:
		return apiVer >= 3
	case apiOffsetCommit:
		return apiVer >= 8
	case apiJoinGroup:
		return apiVer >= 6
	case apiSyncGroup, apiHeartbeat, apiLeaveGroup:
		return apiVer >= 4
	case apiCreateTopics:
		return apiVer >= 5
	case apiDeleteTopics:
		return apiVer >= 4
	case apiInitProducerID:
		return apiVer >= 2
	default:
		return false
	}
}

// flexResponseHeader mirrors flexRequestHeader except ApiVersions, whose response
// header is never flexible (KIP-511).
func flexResponseHeader(apiKey, apiVer int) bool {
	if apiKey == apiApiVersions {
		return false
	}
	return flexRequestHeader(apiKey, apiVer)
}

func skipTags(buf *wire.Buffer) error {
	n, err := buf.ReadUvarint()
	if err != nil {
		return err
	}
	for i := uint(0); i < n; i++ {
		if _, err := buf.ReadUvarint(); err != nil { // tag
			return err
		}
		sz, err := buf.ReadUvarint() // size
		if err != nil {
			return err
		}
		buf.I += int(sz)
	}
	return nil
}

func splitHostPort(a net.Addr) (string, int32) {
	host, portStr, err := net.SplitHostPort(a.String())
	if err != nil {
		return "127.0.0.1", 0
	}
	p := int32(0)
	for _, c := range portStr {
		p = p*10 + int32(c-'0')
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return host, p
}
