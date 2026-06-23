package gokafka

import (
	"context"
	"sync"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// Handler processes a single consumed record. Return error to stop the runner.
type Handler func(ctx context.Context, record Record) error

// Run starts concurrent consumers with heartbeat. Commits offsets only after successful handler completion.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	if err := c.client.requireOpen(); err != nil {
		return err
	}
	defer c.stopHeartbeat()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		default:
		}

		recs, err := c.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break loop
			}
			return err
		}
		if len(recs) == 0 {
			continue
		}

		processed, err := c.processRecords(ctx, recs, h)
		if err != nil {
			return err
		}

		if c.client.cfg.Consumer.AutoCommit {
			if err := c.Commit(ctx, processed...); err != nil {
				return err
			}
		}
	}

	return c.Leave(ctx)
}

func (c *Consumer) processRecords(ctx context.Context, recs []Record, h Handler) ([]Record, error) {
	workers := c.client.cfg.Concurrency.ConsumerWorkers
	if workers <= 0 {
		workers = 1
	}
	if workers >= len(recs) {
		return c.processRecordsParallel(ctx, recs, h)
	}

	type result struct {
		rec Record
		err error
	}
	jobs := make(chan Record, len(recs))
	results := make(chan result, len(recs))

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for rec := range jobs {
				if err := h(ctx, rec); err != nil {
					results <- result{err: err}
					continue
				}
				results <- result{rec: rec}
			}
		}()
	}

	for _, rec := range recs {
		jobs <- rec
	}
	close(jobs)
	wg.Wait()
	close(results)

	var processed []Record
	for res := range results {
		if res.err != nil {
			return nil, res.err
		}
		processed = append(processed, res.rec)
	}
	return processed, nil
}

func (c *Consumer) processRecordsParallel(ctx context.Context, recs []Record, h Handler) ([]Record, error) {
	var (
		wg        sync.WaitGroup
		mu        sync.Mutex
		processed []Record
		errOnce   sync.Once
		handlerErr error
	)
	wg.Add(len(recs))
	for _, rec := range recs {
		rec := rec
		go func() {
			defer wg.Done()
			if err := h(ctx, rec); err != nil {
				errOnce.Do(func() { handlerErr = err })
				return
			}
			mu.Lock()
			processed = append(processed, rec)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return processed, handlerErr
}

func (c *Consumer) heartbeatLoop(ctx context.Context) {
	interval := c.client.cfg.Consumer.HeartbeatInterval
	if interval <= 0 {
		interval = 3 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = c.heartbeat(ctx)
		}
	}
}

func (c *Consumer) heartbeat(ctx context.Context) error {
	c.mu.Lock()
	memberID := c.memberID
	generation := c.generation
	group := c.group
	c.mu.Unlock()
	if memberID == "" {
		return nil
	}
	coord, err := c.coordinator(ctx)
	if err != nil {
		return err
	}
	body := protocol.EncodeHeartbeatRequest(group, memberID, generation)
	rb, err := c.client.cluster.Request(ctx, coord, protocol.APIHeartbeat, protocol.VerHeartbeat, body)
	if err != nil {
		return err
	}
	code, err := protocol.DecodeHeartbeatResponse(rb)
	if err != nil {
		return err
	}
	if code != 0 {
		return newKafkaError(code, "", 0, "heartbeat failed")
	}
	return nil
}

// Leave sends LeaveGroup on shutdown.
func (c *Consumer) Leave(ctx context.Context) error {
	c.stopHeartbeat()
	if c.memberID == "" {
		return nil
	}
	coord, err := c.coordinator(ctx)
	if err != nil {
		return err
	}
	buf := protocol.EncodeLeaveGroupRequest(c.group, c.memberID)
	_, err = c.client.cluster.Request(ctx, coord, protocol.APILeaveGroup, protocol.VerLeaveGroup, buf)
	return err
}
