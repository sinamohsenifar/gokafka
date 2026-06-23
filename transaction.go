package gokafka

import (
	"context"
	"errors"
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
	if err := tp.init(ctx); err != nil {
		return nil, err
	}
	tp.open = true
	return tp, nil
}

func (t *TransactionalProducer) init(ctx context.Context) error {
	body := protocol.EncodeInitProducerID(&t.txnID, t.client.cfg.transactionTimeoutMs())
	var pid protocol.ProducerID
	err := retryRetriable(ctx, t.client.cfg.Retry, func() error {
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
	if err := t.ensurePartitions(ctx, records); err != nil {
		return nil, err
	}
	topics := uniqueTopics(records)
	var results []ProduceRecordResult
	err := retryRetriable(ctx, t.client.cfg.Retry, func() error {
		if err := t.client.cluster.RefreshIfStale(ctx, topics, false); err != nil {
			return err
		}
		res, err := t.prod.sendRecords(ctx, records, recordSendOpts{
			pid: &t.pid, idState: t.idState, transactional: true,
		})
		if err != nil {
			return err
		}
		results = res
		return nil
	})
	return results, err
}

// SendOffsetsToTxn commits consumer group offsets as part of the open transaction (consume-transform-produce).
func (t *TransactionalProducer) SendOffsetsToTxn(ctx context.Context, groupID string, offsets map[string]map[int32]int64) error {
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
	if err := t.ensureGroupOffsets(ctx, groupID, offsets); err != nil {
		return err
	}
	committed := make([]protocol.TxnCommittedOffset, 0)
	for topic, parts := range offsets {
		for part, off := range parts {
			committed = append(committed, protocol.TxnCommittedOffset{
				Topic: topic, Partition: part, Offset: off,
			})
		}
	}
	body := protocol.EncodeTxnOffsetCommit(t.txnID, groupID, t.pid.ID, t.pid.Epoch, committed)
	rb, err := t.client.cluster.Request(ctx, t.coordinator, protocol.APITxnOffsetCommit, protocol.VerTxnOffsetCommit, body)
	if err != nil {
		return err
	}
	code, err := protocol.DecodeTxnOffsetCommit(rb)
	if err != nil {
		return err
	}
	if code != 0 {
		return newKafkaError(code, "", 0, "txn offset commit failed")
	}
	return nil
}

func (t *TransactionalProducer) ensureGroupOffsets(ctx context.Context, groupID string, offsets map[string]map[int32]int64) error {
	if _, ok := t.registeredGroups[groupID]; ok {
		return nil
	}
	topics := offsetsToTxnTopics(offsets)
	body := protocol.EncodeAddOffsetsToTxn(t.txnID, t.pid.ID, t.pid.Epoch, []protocol.TxnGroupOffsets{{
		GroupID: groupID,
		Topics:  topics,
	}})
	rb, err := t.client.cluster.Request(ctx, t.coordinator, protocol.APIAddOffsetsToTxn, protocol.VerAddOffsetsToTxn, body)
	if err != nil {
		return err
	}
	code, err := protocol.DecodeAddOffsetsToTxn(rb)
	if err != nil {
		return err
	}
	if code != 0 {
		return newKafkaError(code, "", 0, "add offsets to transaction failed")
	}
	t.registeredGroups[groupID] = struct{}{}
	return nil
}

func offsetsToTxnTopics(offsets map[string]map[int32]int64) []protocol.TxnTopicPartitions {
	out := make([]protocol.TxnTopicPartitions, 0, len(offsets))
	for topic, parts := range offsets {
		partsList := make([]int32, 0, len(parts))
		for p := range parts {
			partsList = append(partsList, p)
		}
		out = append(out, protocol.TxnTopicPartitions{Topic: topic, Partitions: partsList})
	}
	return out
}

func (t *TransactionalProducer) ensurePartitions(ctx context.Context, records []Record) error {
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
	rb, err := t.client.cluster.Request(ctx, t.coordinator, protocol.APIAddPartitionsTxn, protocol.VerAddPartitionsTxn, body)
	if err != nil {
		return err
	}
	code, err := protocol.DecodeAddPartitionsToTxn(rb)
	if err != nil {
		return err
	}
	if code != 0 {
		return newKafkaError(code, "", 0, "add partitions to transaction failed")
	}
	return nil
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
	body := protocol.EncodeEndTxn(t.txnID, t.pid.ID, t.pid.Epoch, commit)
	rb, err := t.client.cluster.Request(ctx, t.coordinator, protocol.APIEndTxn, protocol.VerEndTxn, body)
	if err != nil {
		return err
	}
	code, err := protocol.DecodeEndTxn(rb)
	if err != nil {
		return err
	}
	if code != 0 {
		return newKafkaError(code, "", 0, "end transaction failed")
	}
	t.open = false
	if !commit {
		return ErrTransactionAborted
	}
	return nil
}
