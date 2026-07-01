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
	// txnPID caches the transactional producer id/epoch across sequential
	// transactions under KIP-890 TV2: EndTxn v5 returns the server-bumped epoch,
	// which the next BeginTransaction reuses instead of re-running InitProducerID.
	txnPID      protocol.ProducerID
	txnPIDValid bool
}

// cacheTxnPID stores the TV2 server-bumped transactional producer id for reuse.
func (p *Producer) cacheTxnPID(pid protocol.ProducerID) {
	p.pidMu.Lock()
	p.txnPID = pid
	p.txnPIDValid = true
	p.pidMu.Unlock()
}

// clearTxnPID invalidates the cached transactional producer id (uncertain state
// or non-TV2 path), forcing the next BeginTransaction to re-initialize.
func (p *Producer) clearTxnPID() {
	p.pidMu.Lock()
	p.txnPIDValid = false
	p.pidMu.Unlock()
}

// cachedTxnPID returns the cached transactional producer id, if valid.
func (p *Producer) cachedTxnPID() (protocol.ProducerID, bool) {
	p.pidMu.Lock()
	defer p.pidMu.Unlock()
	return p.txnPID, p.txnPIDValid
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

	results := make([]ProduceRecordResult, len(records))
	acked := make([]bool, len(records))
	var frozen []Record
	err := retryRetriable(ctx, p.client.cfg.Retry, func() error {
		if err := p.client.cluster.RefreshIfStale(ctx, topics, false); err != nil {
			span.RecordError(err)
			return err
		}
		// Freeze partition assignment once (after the first metadata refresh) and
		// reuse it across retries so the partitioner never re-runs on a record.
		if frozen == nil {
			f, err := p.freezePartitions(records)
			if err != nil {
				span.RecordError(err)
				return err
			}
			frozen = f
		}
		// Send only records not yet acknowledged by a broker. On a partial
		// multi-broker failure this re-sends just the failed partitions; a
		// partition another broker already committed is never re-sent (which,
		// with idempotence off, would duplicate it).
		pending, origIdx := pendingRecords(frozen, acked)
		res, err := p.sendOnce(ctx, pending)
		mergeAcked(results, acked, origIdx, res)
		if err != nil {
			if IsRetriable(err) {
				_ = p.client.cluster.Refresh(ctx, topics)
			}
			// OUT_OF_ORDER_SEQUENCE / INVALID_PRODUCER_EPOCH are deliberately NOT
			// retriable and are surfaced to the caller (fatal for the idempotent
			// producer, abortable for the transactional one). They must never
			// trigger a producer-id reset + full re-send: the broker may have
			// already committed part of this send, and re-sending under a fresh
			// producer id would duplicate those records — the exact guarantee the
			// idempotent producer exists to provide. On a fatal error the caller
			// must treat the whole send as poisoned (results may be partial).
			// Recovery is the caller's: recreate the producer or abort the txn.
			span.RecordError(err)
			span.SetStatus(observe.StatusError, err.Error())
			p.client.observe.Log(ctx, observe.LevelError, "produce failed", observe.Error(err))
			return err
		}
		return nil
	})
	return results, err
}

// pendingRecords returns the records not yet acknowledged (acked[i] == false)
// together with their original indices, for a subset re-send.
func pendingRecords(frozen []Record, acked []bool) (pending []Record, origIdx []int) {
	pending = make([]Record, 0, len(frozen))
	origIdx = make([]int, 0, len(frozen))
	for i, r := range frozen {
		if !acked[i] {
			pending = append(pending, r)
			origIdx = append(origIdx, i)
		}
	}
	return pending, origIdx
}

// mergeAcked copies broker-acknowledged results (Topic != "" — set only for
// partitions the broker committed) from a subset send back into the full results
// slice by original index, marking those records acked so a retry skips them.
func mergeAcked(results []ProduceRecordResult, acked []bool, origIdx []int, res []ProduceRecordResult) {
	for k := range res {
		if res[k].Topic != "" {
			results[origIdx[k]] = res[k]
			acked[origIdx[k]] = true
		}
	}
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
		inputByKey[key] = append(inputByKey[key], indexedRecord{idx: i, rec: r})
		byBroker[leader] = append(byBroker[leader], pr)
	}

	// One batch per partition holding ALL that partition's records. The
	// idempotent sequence block a batch consumes is its record count, not one:
	// encodeRecordBatch stamps baseSequence..baseSequence+N-1 for N records, so
	// the broker expects the next batch at baseSequence+N. Reserving only 1 would
	// leave idState N-1 sequences behind, and every later batch on that partition
	// would be rejected OUT_OF_ORDER_SEQUENCE (and, pre-fix, re-sent under a fresh
	// producer id — silent duplication). Reserve exactly len(records) per key.
	partBatches := make(map[partKey]int, len(inputByKey))
	for k, recs := range inputByKey {
		partBatches[k] = len(recs)
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

	// Collect the FAILED partitions across brokers. A partition one broker
	// acknowledged stays acked (its result is filled, its sequence block kept);
	// only failed partitions are rolled back and returned to the caller for
	// retry. This is what stops a partial multi-broker failure from re-sending —
	// and duplicating — records another broker already committed.
	failed := map[partKey]int{}
	var sendErr error
	markFailed := func(keys []partKey, err error) {
		for _, k := range keys {
			if n, ok := partBatches[k]; ok {
				failed[k] = n
			}
		}
		if err == nil {
			return
		}
		// Prefer a non-retriable (fatal) error so it surfaces instead of looping.
		if sendErr == nil || (IsRetriable(sendErr) && !IsRetriable(err)) {
			sendErr = err
		}
	}

	if len(nodes) == 1 {
		fk, err := p.produceToBroker(ctx, nodes[0], byBroker[nodes[0]], inputByKey, settings, results)
		markFailed(fk, err)
	} else {
		type brokerOut struct {
			failed []partKey
			err    error
		}
		outs := make([]brokerOut, len(nodes))
		var wg sync.WaitGroup
		for i, node := range nodes {
			wg.Add(1)
			go func(i int, node int32) {
				defer wg.Done()
				outs[i].failed, outs[i].err = p.produceToBroker(ctx, node, byBroker[node], inputByKey, settings, results)
			}(i, node)
		}
		wg.Wait()
		for _, o := range outs {
			markFailed(o.failed, o.err)
		}
	}

	rollbackPartitions(failed)
	// Return partial results (acked partitions filled; unacked entries zero) with
	// the representative error. The caller retries only the unacked records.
	return results, sendErr
}

// produceToBroker sends one broker's batch and records the per-partition
// outcome. It writes results[idx] for every partition the broker acknowledged
// (ErrorCode == 0) and returns the partition keys that FAILED plus a
// representative error (a non-retriable/fatal code is preferred so it surfaces
// rather than being retried). It never returns early on the first bad partition:
// the caller must know exactly which partitions committed so it can roll back
// and re-send only the failed ones — re-sending a committed partition would
// duplicate it (with idempotence off) or draw DUPLICATE_SEQUENCE (with it on).
func (p *Producer) produceToBroker(
	ctx context.Context,
	node int32,
	batch []protocol.ProduceRecord,
	inputByKey map[partKey][]indexedRecord,
	settings protocol.ProduceSettings,
	results []ProduceRecordResult,
) ([]partKey, error) {
	ver := p.client.cluster.NegotiatedVersion(protocol.APIProduce, protocol.VerProduce)
	body, err := protocol.EncodeProduceRequest(ver, batch, settings)
	if err != nil {
		return brokerPartKeys(batch), err
	}
	rb, err := p.client.cluster.Request(ctx, node, protocol.APIProduce, ver, body)
	if err != nil {
		// Whole request failed: nothing on this broker was acknowledged.
		p.client.observe.Metrics.OnProduce(0, err)
		return brokerPartKeys(batch), err
	}
	brokerResults, err := protocol.DecodeProduceResponse(ver, rb)
	if err != nil {
		return brokerPartKeys(batch), err
	}
	var failed []partKey
	var firstErr, fatalErr error
	for _, res := range brokerResults {
		pk := partKey{res.Topic, res.Partition}
		if res.ErrorCode != 0 {
			ke := newKafkaError(res.ErrorCode, res.Topic, res.Partition, "produce failed")
			p.client.observe.Metrics.OnProduce(0, ke)
			failed = append(failed, pk)
			if firstErr == nil {
				firstErr = ke
			}
			if !IsRetriable(ke) {
				fatalErr = ke
			}
			continue
		}
		inputs := inputByKey[pk]
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
	if fatalErr != nil {
		return failed, fatalErr
	}
	return failed, firstErr
}

// brokerPartKeys returns the distinct partition keys in a broker's batch, used to
// mark every partition failed when the whole request (transport/decode) fails.
func brokerPartKeys(batch []protocol.ProduceRecord) []partKey {
	seen := map[partKey]struct{}{}
	out := make([]partKey, 0, len(batch))
	for _, r := range batch {
		k := partKey{r.Topic, r.Partition}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	return out
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
		// Explicit (or frozen) partition whose leader is momentarily unknown
		// (metadata gap / in-flight election). Never fall through to the
		// partitioner — that would re-partition a record whose partition was
		// deliberately chosen (or frozen for retry). Surface a retriable error so
		// the produce loop refreshes metadata and re-resolves the leader.
		return 0, 0, newKafkaError(int16(ErrCodeLeaderNotAvail), r.Topic, r.Partition, "leader not available")
	}
	idx := p.partitioner.Partition(r.Key, len(parts))
	pm := parts[idx]
	return pm.Partition, pm.Leader, nil
}

// freezePartitions resolves each keyless/unspecified record's partition ONCE and
// returns a copy with the chosen partition written back. Produce retries reuse
// the frozen copy so the partitioner never runs again on a record: a stateful
// partitioner (RoundRobin advances a shared counter per call) would otherwise
// route the same record to a different partition on a retry — reordering it, or
// duplicating it onto two partitions when an earlier attempt already committed.
// Records with an explicit partition (>= 0) are copied unchanged. The input
// slice is never mutated (it may alias the caller's backing array).
func (p *Producer) freezePartitions(records []Record) ([]Record, error) {
	frozen := make([]Record, len(records))
	for i, r := range records {
		if r.Partition < 0 {
			part, _, err := p.resolvePartition(r)
			if err != nil {
				return nil, err
			}
			r.Partition = part
		}
		frozen[i] = r
	}
	return frozen, nil
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
