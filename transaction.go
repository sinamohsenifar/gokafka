package gokafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sinamohsenifar/gokafka/internal/produce"
	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// TransactionalProducer provides Kafka exactly-once semantics (EOS) within a transaction boundary.
type TransactionalProducer struct {
	client           *Client
	prod             *Producer
	txnID            string
	pid              protocol.ProducerID
	idState          *produce.State
	registered       map[partKey]struct{}
	registeredGroups map[string]struct{}
	coordinator      int32
	mu               sync.Mutex
	open             bool
	// tv2 is set when the cluster has finalized transaction.version >= 2
	// (KIP-890 transactions v2). Under TV2 the partition leader registers data
	// partitions with the transaction implicitly on the Produce path (Produce
	// v12+ carries the transactional id), so the client skips the explicit
	// AddPartitionsToTxn round-trip on the produce hot path. (The consumer-group
	// offsets registration is NOT implicit and still uses AddOffsetsToTxn — see
	// ensureGroupOffsets.) The producer epoch is bumped server-side on EndTxn;
	// GoKafka re-initializes the producer id on each BeginTransaction, so it
	// always acquires a fresh, valid epoch without EndTxn v5 epoch adoption.
	tv2 bool
}

// BeginTransaction initializes producer id and starts a transaction.
func (p *Producer) BeginTransaction(ctx context.Context) (*TransactionalProducer, error) {
	if err := p.client.requireOpen(); err != nil {
		return nil, err
	}
	txnID := p.client.cfg.transactionalID()
	if txnID == "" {
		return nil, ErrNoTransactionalID
	}
	tp := &TransactionalProducer{
		client:           p.client,
		prod:             p,
		txnID:            txnID,
		registered:       map[partKey]struct{}{},
		registeredGroups: map[string]struct{}{},
	}
	tv2 := false
	if lvl, ok := p.client.BrokerFeature("transaction.version"); ok && lvl >= 2 {
		tv2 = true
	}
	// KIP-890 TV2: reuse the producer id/epoch carried over from the previous
	// transaction's EndTxn v5 response, skipping a fresh InitProducerID. The
	// coordinator is still needed for the EndTxn round-trip.
	if cached, ok := p.cachedTxnPID(); tv2 && ok {
		coord, err := p.client.cluster.TransactionCoordinator(ctx, txnID)
		if err != nil {
			return nil, err
		}
		tp.coordinator = coord
		tp.pid = cached
		tp.idState = produce.NewState(cached)
	} else if err := tp.init(ctx); err != nil {
		return nil, err
	}
	tp.tv2 = tv2
	tp.open = true
	return tp, nil
}

func (t *TransactionalProducer) init(ctx context.Context) error {
	body := protocol.EncodeInitProducerID(&t.txnID, t.client.cfg.transactionTimeoutMs())
	var pid protocol.ProducerID
	// Patient retry: the transaction coordinator may still be loading
	// __transaction_state right after broker startup (COORDINATOR_LOAD_IN_PROGRESS
	// / NOT_COORDINATOR / COORDINATOR_NOT_AVAILABLE).
	err := retryRetriable(ctx, coordinatorRetry(t.client.cfg.Retry), func() error {
		coord, err := t.client.cluster.TransactionCoordinator(ctx, t.txnID)
		if err != nil {
			return err
		}
		t.coordinator = coord
		rb, err := t.client.cluster.Request(ctx, coord, protocol.APIInitProducerID, protocol.VerInitProducerID, body)
		if err != nil {
			return err
		}
		pid, err = protocol.DecodeInitProducerID(rb)
		if err != nil {
			var apiErr *protocol.APIError
			if errors.As(err, &apiErr) {
				if protocol.CoordinatorRetriable(apiErr.Code) {
					t.client.cluster.Invalidate(coord)
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
	t.pid = pid
	t.idState = produce.NewState(pid)
	return nil
}

// ProduceWithinTxn produces records as part of the open transaction.
func (t *TransactionalProducer) ProduceWithinTxn(ctx context.Context, records ...Record) error {
	_, err := t.ProduceWithinTxnResult(ctx, records...)
	return err
}

// ProduceWithinTxnResult produces records and returns broker offsets.
func (t *TransactionalProducer) ProduceWithinTxnResult(ctx context.Context, records ...Record) ([]ProduceRecordResult, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.open {
		return nil, ErrTransactionAborted
	}
	if len(records) == 0 {
		return nil, nil
	}
	// Freeze partition assignment before registering partitions so the partition
	// AddPartitionsToTxn registers is exactly the one the send uses. A stateful
	// partitioner (RoundRobin) would otherwise run once here and again in
	// sendRecords, picking different partitions — the send would then target a
	// partition never added to the transaction.
	frozen, err := t.prod.freezePartitions(records)
	if err != nil {
		return nil, err
	}
	records = frozen
	if err := t.ensurePartitions(ctx, records); err != nil {
		return nil, err
	}
	topics := uniqueTopics(records)
	results := make([]ProduceRecordResult, len(records))
	acked := make([]bool, len(records))
	err = retryRetriable(ctx, t.client.cfg.Retry, func() error {
		if err := t.client.cluster.RefreshIfStale(ctx, topics, false); err != nil {
			return err
		}
		// Re-send only records not yet acknowledged, so a partial multi-broker
		// failure never re-sends a partition already committed.
		pending, origIdx := pendingRecords(records, acked)
		res, err := t.prod.sendRecords(ctx, pending, recordSendOpts{
			pid: &t.pid, idState: t.idState, transactional: true,
		})
		mergeAcked(results, acked, origIdx, res)
		return err
	})
	return results, err
}

// TxnOffsetCommitOptions carries consumer group metadata for SendOffsetsToTxn (TxnOffsetCommit v3+).
type TxnOffsetCommitOptions struct {
	Generation      int32
	MemberID        string
	GroupInstanceID string
}

// SendOffsetsToTxn commits consumer group offsets as part of the open transaction (consume-transform-produce).
func (t *TransactionalProducer) SendOffsetsToTxn(ctx context.Context, groupID string, offsets map[string]map[int32]int64, opts TxnOffsetCommitOptions) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.open {
		return ErrTransactionAborted
	}
	if groupID == "" {
		return ErrNoConsumerGroup
	}
	if len(offsets) == 0 {
		return nil
	}
	if err := t.ensureGroupOffsets(ctx, groupID); err != nil {
		return fmt.Errorf("add offsets to transaction: %w", err)
	}
	committed := make([]protocol.TxnCommittedOffset, 0)
	for topic, parts := range offsets {
		for part, off := range parts {
			committed = append(committed, protocol.TxnCommittedOffset{
				Topic: topic, Partition: part, Offset: off,
			})
		}
	}
	gen := opts.Generation
	if gen == 0 && opts.MemberID == "" && opts.GroupInstanceID == "" {
		gen = -1
	}
	meta := protocol.TxnOffsetCommitMeta{
		Generation:      gen,
		MemberID:        opts.MemberID,
		GroupInstanceID: opts.GroupInstanceID,
	}
	txnVer := t.client.cluster.NegotiatedVersion(protocol.APITxnOffsetCommit, protocol.VerTxnOffsetCommit)
	if txnVer < 3 {
		txnVer = 3
	}
	body := protocol.EncodeTxnOffsetCommit(txnVer, t.txnID, groupID, t.pid.ID, t.pid.Epoch, meta, committed)
	return t.txnCoordRequest(ctx, protocol.APITxnOffsetCommit, txnVer, body,
		func(rb []byte) (int16, error) { return protocol.DecodeTxnOffsetCommit(txnVer, rb) },
		"txn offset commit failed")
}

// txnCoordRequest sends a request to the transaction coordinator and retries
// patiently while the coordinator is loading or being re-elected at startup,
// re-resolving the coordinator on coordinator-retriable errors. decode returns
// the response's top-level error code.
func (t *TransactionalProducer) txnCoordRequest(ctx context.Context, apiKey, ver int16, body []byte, decode func([]byte) (int16, error), what string) error {
	return retryRetriable(ctx, coordinatorRetry(t.client.cfg.Retry), func() error {
		rb, err := t.client.cluster.Request(ctx, t.coordinator, apiKey, ver, body)
		if err != nil {
			return err
		}
		code, err := decode(rb)
		if err != nil {
			return err
		}
		if code != 0 {
			if protocol.CoordinatorRetriable(code) {
				t.client.cluster.Invalidate(t.coordinator)
				if c, e := t.client.cluster.TransactionCoordinator(ctx, t.txnID); e == nil {
					t.coordinator = c
				}
			}
			return newKafkaError(code, "", 0, what)
		}
		return nil
	})
}

func (t *TransactionalProducer) ensureGroupOffsets(ctx context.Context, groupID string) error {
	// Note: even under TV2 the consumer-group offsets topic must be registered
	// with the transaction via AddOffsetsToTxn before TxnOffsetCommit — unlike
	// data partitions, the offsets registration is not implicit on the commit
	// path (the broker returns INVALID_TXN_STATE otherwise). So this RPC is kept
	// regardless of transaction.version.
	if _, ok := t.registeredGroups[groupID]; ok {
		return nil
	}
	addVer := t.client.cluster.NegotiatedVersion(protocol.APIAddOffsetsToTxn, protocol.VerAddOffsetsToTxn)
	body := protocol.EncodeAddOffsetsToTxn(addVer, t.txnID, t.pid.ID, t.pid.Epoch, groupID)
	if err := t.txnCoordRequest(ctx, protocol.APIAddOffsetsToTxn, addVer, body,
		func(rb []byte) (int16, error) { return protocol.DecodeAddOffsetsToTxn(addVer, rb) },
		"add offsets to transaction failed"); err != nil {
		return err
	}
	t.registeredGroups[groupID] = struct{}{}
	return nil
}

func (t *TransactionalProducer) ensurePartitions(ctx context.Context, records []Record) error {
	if t.tv2 {
		// KIP-890 TV2: the partition leader registers the partition with the
		// transaction implicitly on the first transactional Produce (v12+
		// carries the transactional id), so the client skips AddPartitionsToTxn.
		return nil
	}
	pending := map[string][]int32{}
	var newKeys []partKey
	for _, r := range records {
		part, _, err := t.prod.resolvePartition(r)
		if err != nil {
			return err
		}
		k := partKey{r.Topic, part}
		if _, ok := t.registered[k]; ok {
			continue
		}
		t.registered[k] = struct{}{}
		pending[r.Topic] = append(pending[r.Topic], part)
		newKeys = append(newKeys, k)
	}
	if len(pending) == 0 {
		return nil
	}
	topics := make([]protocol.TxnTopicPartitions, 0, len(pending))
	for topic, parts := range pending {
		topics = append(topics, protocol.TxnTopicPartitions{Topic: topic, Partitions: parts})
	}
	if err := t.addPartitions(ctx, topics); err != nil {
		for _, k := range newKeys {
			delete(t.registered, k)
		}
		return err
	}
	return nil
}

func (t *TransactionalProducer) addPartitions(ctx context.Context, topics []protocol.TxnTopicPartitions) error {
	body := protocol.EncodeAddPartitionsToTxn(t.txnID, t.pid.ID, t.pid.Epoch, topics)
	return t.txnCoordRequest(ctx, protocol.APIAddPartitionsTxn, protocol.VerAddPartitionsTxn, body,
		func(rb []byte) (int16, error) { return protocol.DecodeAddPartitionsToTxn(rb) },
		"add partitions to transaction failed")
}

// Commit commits the transaction.
func (t *TransactionalProducer) Commit(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.endTxn(ctx, true)
}

// Abort rolls back the transaction.
func (t *TransactionalProducer) Abort(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.endTxn(ctx, false)
}

func (t *TransactionalProducer) endTxn(ctx context.Context, commit bool) error {
	if !t.open {
		return ErrTransactionAborted
	}
	ver := t.client.cluster.NegotiatedVersion(protocol.APIEndTxn, protocol.VerEndTxn)
	if ver <= 0 {
		ver = protocol.VerEndTxn
	}
	var res protocol.EndTxnResult
	err := retryRetriable(ctx, coordinatorRetry(t.client.cfg.Retry), func() error {
		body := protocol.EncodeEndTxn(ver, t.txnID, t.pid.ID, t.pid.Epoch, commit)
		rb, rerr := t.client.cluster.Request(ctx, t.coordinator, protocol.APIEndTxn, ver, body)
		if rerr != nil {
			return rerr
		}
		r, derr := protocol.DecodeEndTxn(ver, rb)
		if derr != nil {
			return derr
		}
		if r.Code != 0 {
			if protocol.CoordinatorRetriable(r.Code) {
				t.client.cluster.Invalidate(t.coordinator)
				if c, e := t.client.cluster.TransactionCoordinator(ctx, t.txnID); e == nil {
					t.coordinator = c
				}
			}
			return newKafkaError(r.Code, "", 0, "end transaction failed")
		}
		res = r
		return nil
	})
	if err != nil {
		// The transaction outcome is uncertain; drop any cached producer id so the
		// next BeginTransaction re-initializes a fresh, valid epoch.
		t.prod.clearTxnPID()
		return err
	}
	t.open = false
	// KIP-890 TV2: EndTxn v5 returns the server-bumped producer id/epoch. Adopt it
	// and cache it on the producer so the next transaction reuses it without a
	// fresh InitProducerID round-trip. Older versions return -1 here.
	if t.tv2 && res.ProducerID >= 0 && res.ProducerEpoch >= 0 {
		t.pid = protocol.ProducerID{ID: res.ProducerID, Epoch: res.ProducerEpoch}
		t.prod.cacheTxnPID(t.pid)
	} else {
		t.prod.clearTxnPID()
	}
	if !commit {
		return ErrTransactionAborted
	}
	return nil
}

// ProducerID returns the current transactional producer id and epoch. Across
// sequential transactions on a TV2 cluster the id stays constant while the epoch
// increases (the broker bumps it on each EndTxn); useful for diagnostics.
func (t *TransactionalProducer) ProducerID() (id int64, epoch int16) {
	return t.pid.ID, t.pid.Epoch
}
