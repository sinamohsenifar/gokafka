package gokafka

import (
	"context"
	"sync"
	"time"
)

// ProduceResult is the delivery report for async produce.
type ProduceResult struct {
	Record Record
	Result ProduceRecordResult
	Err    error
}

// AsyncProducer sends records concurrently via a buffered channel.
type AsyncProducer struct {
	client *Client
	prod   *Producer
	in     chan Record
	out    chan ProduceResult
	wg     sync.WaitGroup
	once   sync.Once
	closed chan struct{}
}

// NewAsyncProducer creates a producer with worker pool. Call Run to start workers.
func (c *Client) NewAsyncProducer() *AsyncProducer {
	buf := c.cfg.Concurrency.ChannelBuffer
	if buf <= 0 {
		buf = 256
	}
	return &AsyncProducer{
		client: c,
		prod:   c.Producer(),
		in:     make(chan Record, buf),
		out:    make(chan ProduceResult, buf),
		closed: make(chan struct{}),
	}
}

// Input returns the channel for sending records.
func (a *AsyncProducer) Input() chan<- Record { return a.in }

// Results returns delivery reports with offsets when successful.
func (a *AsyncProducer) Results() <-chan ProduceResult { return a.out }

// Run starts producer workers until ctx is cancelled.
func (a *AsyncProducer) Run(ctx context.Context) {
	workers := a.client.cfg.Concurrency.ProducerWorkers
	for i := 0; i < workers; i++ {
		a.wg.Add(1)
		go a.worker(ctx)
	}
	a.wg.Wait()
	close(a.out)
}

func (a *AsyncProducer) worker(ctx context.Context) {
	defer a.wg.Done()
	batchSize := a.client.cfg.Producer.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	linger := a.client.cfg.Producer.Linger
	if linger <= 0 {
		linger = 5 * time.Millisecond
	}

	batch := make([]Record, 0, batchSize)
	var flushTimer *time.Timer
	var flushCh <-chan time.Time

	stopTimer := func() {
		if flushTimer == nil {
			return
		}
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}
		flushCh = nil
	}

	armTimer := func() {
		if flushTimer == nil {
			flushTimer = time.NewTimer(linger)
			flushCh = flushTimer.C
			return
		}
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}
		flushTimer.Reset(linger)
		flushCh = flushTimer.C
	}

	flush := func() {
		if len(batch) == 0 {
			return
		}
		results, err := a.prod.ProduceSyncResult(ctx, batch...)
		for i, r := range batch {
			res := ProduceResult{Record: r, Err: err}
			if err == nil && i < len(results) {
				res.Result = results[i]
			}
			select {
			case a.out <- res:
			case <-ctx.Done():
				return
			}
		}
		batch = batch[:0]
		stopTimer()
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			if flushTimer != nil {
				flushTimer.Stop()
			}
			return
		case <-a.closed:
			flush()
			if flushTimer != nil {
				flushTimer.Stop()
			}
			return
		case r, ok := <-a.in:
			if !ok {
				flush()
				if flushTimer != nil {
					flushTimer.Stop()
				}
				return
			}
			batch = append(batch, r)
			if len(batch) == 1 {
				armTimer()
			}
			if len(batch) >= batchSize {
				flush()
			}
		case <-flushCh:
			flush()
		}
	}
}

// Close stops accepting new records.
func (a *AsyncProducer) Close() {
	a.once.Do(func() {
		close(a.closed)
		close(a.in)
	})
}

// Send enqueues a record.
func (a *AsyncProducer) Send(ctx context.Context, r Record) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-a.closed:
		return ErrClosed
	case a.in <- r:
		return nil
	}
}
