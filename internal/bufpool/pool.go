package bufpool

import (
	"sync"
)

const defaultCap = 64 << 10

// Pool reuses byte slices for fetch/decompress hot paths.
type Pool struct {
	pool sync.Pool
}

// Default is the shared buffer pool for protocol fetch/decompress.
var Default = New(defaultCap)

// New creates a pool whose slices start with at least minCap capacity.
func New(minCap int) *Pool {
	if minCap <= 0 {
		minCap = defaultCap
	}
	return &Pool{
		pool: sync.Pool{
			New: func() any {
				b := make([]byte, 0, minCap)
				return &b
			},
		},
	}
}

// Get returns a pooled slice with length 0.
func (p *Pool) Get() []byte {
	if p == nil {
		return make([]byte, 0, defaultCap)
	}
	bp := p.pool.Get().(*[]byte)
	*bp = (*bp)[:0]
	return *bp
}

// Put returns a slice to the pool when capacity is worth retaining.
func (p *Pool) Put(b []byte) {
	if p == nil || cap(b) < defaultCap/4 {
		return
	}
	bp := b
	p.pool.Put(&bp)
}

// Grow returns a slice with at least n bytes length, reusing b when possible.
func Grow(b []byte, n int) []byte {
	if cap(b) >= n {
		return b[:n]
	}
	out := make([]byte, n)
	copy(out, b)
	return out
}
