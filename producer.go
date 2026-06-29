package gokafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/produce"
	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/observe"
)

// Producer publishes records synchronously with partitioning, idempotency, and retries.
type Producer struct {
	client      *Client
	partitioner Partitioner
	pid         protocol.ProducerID
	idState     *produce.State
	pidMu       sync.Mutex
	pidReady    bool
}

// ProduceSync sends records and waits for broker acknowledgement.
func (p *Producer) ProduceSync(ctx context.Context, records ...Record) error {
	_, err := p.ProduceSyncResult(ctx, records...)
	return err
}

// ProduceSyncResult sends records and returns broker offsets on success.
func (p *Producer) ProduceSyncResult(ctx context.Context, records ...Record) ([]ProduceRecordResult, error) {
	if err := p.client.requireOpen(); err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}
	if err := p.ensureProducerID(ctx); err != nil {
		return nil, err
	}

	topics := uniqueTopics(records)
	ctx, span := p.client.observe.StartSpan(ctx, "gokafka.produce", observe.String("messaging.system", "kafka"))
	defer span.End()

	var results []ProduceRecordResult
	err := retryRetriable(ctx, p.client.cfg.Retry, func() error {
		if err := p.client.cluster.RefreshIfStale(ctx, topics, false); err != nil {
			span.RecordError(err)
			return err
		}
		res, err := p.sendOnce(ctx, records)
		if err != nil {
			if IsRetriable(err) {
				_ = p.client.cluster.Refresh(ctx, topics)
			}
			if p.shouldResetProducerID(err) {
				p.resetProducerID()
				if err2 := p.ensureProducerID(ctx); err2 != nil {
					return err2
				}
			}
			span.RecordError(err)
			span.SetStatus(observe.StatusError, err.Error())
			p.client.observe.Log(ctx, observe.LevelError, "produce failed", observe.Error(err))
			return err
		}
		results = res
		return nil
	})
	return results, err
}

func (p *Producer) ensureProducerID(ctx context.Context) error {
	if !p.client.cfg.Producer.Idempotent && p.client.cfg.transactionalID() == "" {
		return nil
	}
	p.pidMu.Lock()
	defer p.pidMu.Unlock()
	if p.pidReady {
		return nil
	}
	txnID := p.client.cfg.transactionalID()
	var txnPtr *string
	if txnID != "" {
		txnPtr = &txnID
	}
	body := protocol.EncodeInitProducerID(txnPtr, p.client.cfg.transactionTimeoutMs())
	var pid protocol.ProducerID
	err := retryRetriable(ctx, coordinatorRetry(p.client.cfg.Retry), func() error {
		var coord int32
		var err error
		if txnID != "" {
			coord, err = p.client.cluster.TransactionCoordinator(ctx, txnID)
			if err != nil {
				return err
			}
		}
		var rb []byte
		if txnID != "" {
			rb, err = p.client.cluster.Request(ctx, coord, protocol.APIInitProducerID, protocol.VerInitProducerID, body)
		} else {
			rb, err = p.client.cluster.RequestViaSeed(ctx, protocol.APIInitProducerID, protocol.VerInitProducerID, body)
		}
		if err != nil {
			return err
		}
		pid, err = protocol.DecodeInitProducerID(rb)
		if err != nil {
			var apiErr *protocol.APIError
			if errors.As(err, &apiErr) {
				if protocol.CoordinatorRetriable(apiErr.Code) && txnID != "" {
					p.client.cluster.Invalidate(coord)
				}
				return newKafkaError(apiErr.Code, "", 0, "init producer id failed")
			}
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	p.pid = pid
	p.idState = produce.NewState(pid)
	p.pidReady = true
	return nil
}

// coordinatorRetry returns a retry policy patient enough to wait out a
// transaction/group coordinator or a partition leader that is still loading or
// being elected right after broker startup or topic creation (errors
// COORDINATOR_LOAD_IN_PROGRESS / NOT_COORDINATOR / COORDINATOR_NOT_AVAILABLE /
// NOT_LEADER_OR_FOLLOWER / LEADER_NOT_AVAILABLE). The default 3-attempt policy
// gives up in ~300ms, too short for a freshly started cluster. The overall wait
// stays bounded by the caller's context.
func coordinatorRetry(base RetryConfig) RetryConfig {
	r := base
	if r.MaxAttempts < 25 {
		r.MaxAttempts = 25
	}
	if r.Backoff <= 0 {
		r.Backoff = 200 * time.Millisecond
	}
	if r.MaxBackoff <= 0 || r.MaxBackoff > time.Second {
		r.MaxBackoff = time.Second
	}
	return r
}

func (p *Producer) resetProducerID() {
	p.pidMu.Lock()
	defer p.pidMu.Unlock()
	p.pidReady = false
	p.idState = nil
}

func (p *Producer) shouldResetProducerID(err error) bool {
	var ke *KafkaError
	if !AsKafkaError(err, &ke) {
		return false
	}
	return ke.Code == ErrCodeInvalidProducerEpoch || ke.Code == ErrCodeOutOfOrderSequence
}

func (p *Producer) produceSettings(seq func(topic string, part int32) int32, pid *protocol.ProducerID, transactional bool) protocol.ProduceSettings {
	settings := protocol.ProduceSettings{
		Acks:             int16(p.client.cfg.Producer.Acks),
		TimeoutMs:        30000,
		Compression:      p.client.cfg.compressionByte(),
		CompressionLevel: p.client.cfg.Producer.CompressionLevel,
		Transactional:    transactional,
	}
	if transactional {
		settings.TransactionalID = p.client.cfg.transactionalID()
		if settings.Acks != int16(AcksAll) {
			settings.Acks = int16(AcksAll)
		}
	}
	if pid != nil {
		settings.ProducerID = pid.ID
		settings.ProducerEpoch = pid.Epoch
		settings.NextSequence = seq
	}
	return settings
}

type recordSendOpts struct {
	pid           *protocol.ProducerID
	idState       *produce.State
	transactional bool
}

type partKey struct {
	topic string
	part  int32
}

func (p *Producer) sendOnce(ctx context.Context, records []Record) ([]ProduceRecordResult, error) {
	p.pidMu.Lock()
	var pid *protocol.ProducerID
	var idState *produce.State
	if p.pidReady {
		pid = &p.pid
		idState = p.idState
	}
	p.pidMu.Unlock()
	return p.sendRecords(ctx, records, recordSendOpts{pid: pid, idState: idState})
}

type indexedRecord struct {
	idx int
	rec Record
}

// sendRecords produces records using optional idempotent producer id/state (for transactions).
func (p *Producer) sendRecords(ctx context.Context, records []Record, opts recordSendOpts) ([]ProduceRecordResult, error) {
	protoByKey := map[partKey][]protocol.ProduceRecord{}
	inputByKey := map[partKey][]indexedRecord{}
	byBroker := map[int32][]protocol.ProduceRecord{}

	for i, r := range records {
		part, leader, err := p.resolvePartition(r)
		if err != nil {
			p.client.observe.Metrics.OnProduce(len(r.Value), err)
			return nil, err
		}
		key := partKey{r.Topic, part}
		pr := protocol.ProduceRecord{
			Topic: r.Topic, Partition: part, Key: r.Key, Value: r.Value,
			Headers: recordHeaders(r.Headers), Timestamp: timeNow(r.Timestamp),
		}
		protoByKey[key] = append(protoByKey[key], pr)
		inputByKey[key] = append(inputByKey[key], indexedRecord{idx: i, rec: r})
		byBroker[leader] = append(byBroker[leader], pr)
	}

	partBatches := map[partKey]int{}
	for k, rs := range protoByKey {
		partBatches[k] = 1
		_ = rs
	}

	seqCursor := map[partKey]int32{}
	if opts.idState != nil {
		for k, n := range partBatches {
			base := opts.idState.ReserveBlock(k.topic, k.part, n)
			seqCursor[k] = base
		}
	}

	rollbackPartitions := func(batches map[partKey]int) {
		if opts.idState == nil {
			return
		}
		for k, n := range batches {
			opts.idState.RollbackBlock(k.topic, k.part, n)
		}
	}

	// nextSeq is invoked from one goroutine per broker during the parallel
	// fan-out below, so the shared seqCursor map must be guarded.
	var seqMu sync.Mutex
	nextSeq := func(topic string, part int32) int32 {
		k := partKey{topic, part}
		seqMu.Lock()
		seq := seqCursor[k]
		seqCursor[k]++
		seqMu.Unlock()
		return seq
	}

	settings := p.produceSettings(nextSeq, opts.pid, opts.transactional)
	results := make([]ProduceRecordResult, len(records))

	nodes := make([]int32, 0, len(byBroker))
	for node := range byBroker {
		nodes = append(nodes, node)
	}
	if len(nodes) == 1 {
		if err := p.produceToBroker(ctx, nodes[0], byBroker[nodes[0]], inputByKey, settings, results); err != nil {
			rollbackPartitions(partBatches)
			return nil, err
		}
		return results, nil
	}

	type brokerOut struct {
		err error
	}
	outs := make([]brokerOut, len(nodes))
	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)
		go func(i int, node int32) {
			defer wg.Done()
			outs[i].err = p.produceToBroker(ctx, node, byBroker[node], inputByKey, settings, results)
		}(i, node)
	}
	wg.Wait()
	for _, o := range outs {
		if o.err != nil {
			rollbackPartitions(partBatches)
			return nil, o.err
		}
	}
	return results, nil
}

func (p *Producer) produceToBroker(
	ctx context.Context,
	node int32,
	batch []protocol.ProduceRecord,
	inputByKey map[partKey][]indexedRecord,
	settings protocol.ProduceSettings,
	results []ProduceRecordResult,
) error {
	ver := p.client.cluster.NegotiatedVersion(protocol.APIProduce, protocol.VerProduce)
	body, err := protocol.EncodeProduceRequest(ver, batch, settings)
	if err != nil {
		return err
	}
	rb, err := p.client.cluster.Request(ctx, node, protocol.APIProduce, ver, body)
	if err != nil {
		p.client.observe.Metrics.OnProduce(0, err)
		return err
	}
	brokerResults, err := protocol.DecodeProduceResponse(ver, rb)
	if err != nil {
		return err
	}
	for _, res := range brokerResults {
		if res.ErrorCode != 0 {
			ke := newKafkaError(res.ErrorCode, res.Topic, res.Partition, "produce failed")
			p.client.observe.Metrics.OnProduce(0, ke)
			return ke
		}
		inputs := inputByKey[partKey{res.Topic, res.Partition}]
		for i, inp := range inputs {
			off := res.Offset
			if len(inputs) > 1 {
				off = res.Offset + int64(i)
			}
			results[inp.idx] = ProduceRecordResult{
				Record: inp.rec, Topic: res.Topic, Partition: res.Partition, Offset: off,
			}
			p.client.observe.Metrics.OnProduce(len(inp.rec.Value), nil)
		}
	}
	return nil
}

func recordHeaders(hdrs []Header) [][2][]byte {
	if len(hdrs) == 0 {
		return nil
	}
	out := make([][2][]byte, len(hdrs))
	for i, h := range hdrs {
		out[i] = [2][]byte{[]byte(h.Key), h.Value}
	}
	return out
}

func timeNow(ts time.Time) time.Time {
	if ts.IsZero() {
		return time.Now()
	}
	return ts
}

func (p *Producer) resolvePartition(r Record) (part int32, leader int32, err error) {
	meta := p.client.cluster.Metadata()
	var parts []protocol.PartitionMeta
	for _, t := range meta.Topics {
		if t.Name == r.Topic {
			parts = t.Partitions
			break
		}
	}
	if len(parts) == 0 {
		return 0, 0, fmt.Errorf("gokafka: unknown topic %s", r.Topic)
	}
	if r.Partition >= 0 {
		if leader, ok := p.client.cluster.LeaderNodeID(r.Topic, r.Partition); ok {
			return r.Partition, leader, nil
		}
	}
	idx := p.partitioner.Partition(r.Key, len(parts))
	pm := parts[idx]
	return pm.Partition, pm.Leader, nil
}

func uniqueTopics(records []Record) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0)
	for _, r := range records {
		if _, ok := seen[r.Topic]; ok {
			continue
		}
		seen[r.Topic] = struct{}{}
		out = append(out, r.Topic)
	}
	return out
}

func retryRetriable(ctx context.Context, cfg RetryConfig, fn func() error) error {
	wait := cfg.Backoff
	if wait <= 0 {
		wait = 100 * time.Millisecond
	}
	maxWait := cfg.MaxBackoff
	if maxWait <= 0 {
		maxWait = 2 * time.Second
	}
	max := cfg.MaxAttempts
	if max < 1 {
		max = 1
	}
	var err error
	for attempt := 0; attempt < max; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err = fn()
		if err == nil {
			return nil
		}
		if !IsRetriable(err) {
			return err
		}
		if attempt == max-1 {
			break
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		wait *= 2
		if wait > maxWait {
			wait = maxWait
		}
	}
	return err
}

// ProduceSettings exposes effective produce wire settings (for diagnostics).
func (p *Producer) ProduceSettings() protocol.ProduceSettings {
	return p.produceSettings(func(string, int32) int32 { return 0 }, nil, false)
}
