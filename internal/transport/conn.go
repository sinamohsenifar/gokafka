package transport

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/auth"
	"github.com/sinamohsenifar/gokafka/internal/bufpool"
	"github.com/sinamohsenifar/gokafka/internal/limits"
	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// Conn is a Kafka broker connection with optional SASL/TLS.
type Conn struct {
	addr              string
	netConn           net.Conn
	reader            *bufio.Reader
	clientID          string
	correlationID     int32
	security          auth.Config
	requestTimeout    time.Duration
	maxResponseBytes  int
}

func Dial(ctx context.Context, addr, clientID string, sec auth.Config, dialTimeout, requestTimeout time.Duration, maxResponseBytes int) (*Conn, error) {
	if dialTimeout <= 0 {
		dialTimeout = 10 * time.Second
	}
	if requestTimeout <= 0 {
		requestTimeout = 30 * time.Second
	}
	if maxResponseBytes <= 0 {
		maxResponseBytes = limits.MaxResponseBytes
	}
	d := net.Dialer{Timeout: dialTimeout}
	nc, err := auth.Dial(ctx, d, addr, sec)
	if err != nil {
		return nil, err
	}
	c := &Conn{
		addr:             addr,
		netConn:          nc,
		reader:           bufio.NewReader(nc),
		clientID:         clientID,
		security:         sec,
		requestTimeout:   requestTimeout,
		maxResponseBytes: maxResponseBytes,
	}
	if sec.SASLEnabled() {
		if err := auth.Handshake(ctx, c, sec); err != nil {
			nc.Close()
			return nil, err
		}
	}
	return c, nil
}

func (c *Conn) Close() error {
	if c.netConn == nil {
		return nil
	}
	return c.netConn.Close()
}

func (c *Conn) Addr() string { return c.addr }

func (c *Conn) Write(b []byte) (int, error) {
	return c.netConn.Write(b)
}

func (c *Conn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

func (c *Conn) nextCorrelationID() int32 {
	return atomic.AddInt32(&c.correlationID, 1)
}

// Request sends a Kafka request and returns the full response frame.
func (c *Conn) Request(ctx context.Context, apiKey, apiVersion int16, body []byte) ([]byte, error) {
	corr := c.nextCorrelationID()
	frame := protocol.EncodeRequest(protocol.RequestHeader{
		APIKey: apiKey, APIVersion: apiVersion, CorrelationID: corr, ClientID: c.clientID,
	}, body)

	deadline, ok := ctx.Deadline()
	if ok {
		_ = c.netConn.SetDeadline(deadline)
	} else {
		_ = c.netConn.SetDeadline(time.Now().Add(c.requestTimeout))
	}
	defer c.netConn.SetDeadline(time.Time{})

	if _, err := c.netConn.Write(frame); err != nil {
		return nil, fmt.Errorf("transport: write: %w", err)
	}

	sizeBuf := bufpool.Default.Get()
	if cap(sizeBuf) < 4 {
		sizeBuf = bufpool.Grow(sizeBuf, 4)
	}
	sizeBuf = sizeBuf[:4]
	if _, err := io.ReadFull(c.reader, sizeBuf); err != nil {
		bufpool.Default.Put(sizeBuf)
		return nil, fmt.Errorf("transport: read size: %w", err)
	}
	size := int(sizeBuf[0])<<24 | int(sizeBuf[1])<<16 | int(sizeBuf[2])<<8 | int(sizeBuf[3])
	if size < 0 || size > c.maxResponseBytes {
		bufpool.Default.Put(sizeBuf)
		return nil, fmt.Errorf("transport: response size %d exceeds limit %d", size, c.maxResponseBytes)
	}
	frameLen := 4 + size
	resp := bufpool.Default.Get()
	if cap(resp) < frameLen {
		bufpool.Default.Put(resp)
		resp = make([]byte, frameLen)
	} else {
		resp = resp[:frameLen]
	}
	copy(resp, sizeBuf)
	bufpool.Default.Put(sizeBuf)
	if _, err := io.ReadFull(c.reader, resp[4:]); err != nil {
		bufpool.Default.Put(resp)
		return nil, fmt.Errorf("transport: read body: %w", err)
	}
	out := make([]byte, len(resp))
	copy(out, resp)
	bufpool.Default.Put(resp)
	return out, nil
}

func ResponseBody(raw []byte) ([]byte, error) {
	return protocol.ResponseBody(raw)
}

// ResponseBodyForAPI strips the Kafka response header for a specific API version.
func ResponseBodyForAPI(raw []byte, apiKey, apiVersion int16) ([]byte, error) {
	return protocol.ResponseBodyForAPI(raw, apiKey, apiVersion)
}
