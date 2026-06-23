package gokafka

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/observe"
)

// Consumer reads from topics using a consumer group.
type Consumer struct {
	mu          sync.Mutex
	client      *Client
	topics      []string
	group       string
	memberID    string
	generation  int32
	coordID     int32
	hasCoord    bool
	assignments []partitionOffset
	listener    RebalanceListener
	paused      map[partKey]struct{}
	hbCancel    context.CancelFunc
}

type partitionOffset struct {
	topic     string
	partition int32
	offset    int64
}

// Poll fetches the next batch of records from partition leaders.
func (c *Consumer) Poll(ctx context.Context) ([]Record, error) {
	if err := c.client.requireOpen(); err != nil {
		return nil, err
	}
	if c.client.cfg.ConsumerGroup == "" {
		return nil, ErrNoConsumerGroup
	}
	c.mu.Lock()
	c.group = c.client.cfg.ConsumerGroup
	needJoin := len(c.assignments) == 0
	c.mu.Unlock()

	if needJoin {
		if err := c.joinAndAssign(ctx); err != nil {
			c.client.observe.Metrics.OnConsume(0, err)
			c.client.observe.Log(ctx, observe.LevelError, "consumer join failed", observe.Error(err))
			return nil, err
		}
	}

	maxPoll := c.client.cfg.Consumer.MaxPollRecords
	if maxPoll <= 0 {
		maxPoll = 500
	}

	byNode := map[int32][]protocol.FetchPartition{}
	c.mu.Lock()
	assignments := append([]partitionOffset(nil), c.assignments...)
	c.mu.Unlock()
	for _, a := range assignments {
		if c.isPaused(a.topic, a.partition) {
			continue
		}
		b, err := c.client.cluster.LeaderBroker(a.topic, a.partition)
		if err != nil {
			return nil, err
		}
		byNode[b.NodeID] = append(byNode[b.NodeID], protocol.FetchPartition{
			Topic: a.topic, Partition: a.partition, Offset: a.offset, MaxBytes: 1 << 20,
		})
	}

	isolation := int8(0)
	if c.client.cfg.Consumer.IsolationLevel == IsolationReadCommitted {
		isolation = 1
	}

	group := c.group
	nodes := make([]int32, 0, len(byNode))
	for node := range byNode {
		nodes = append(nodes, node)
	}
	if len(nodes) == 1 {
		recs, err := c.fetchFromBroker(ctx, group, nodes[0], byNode[nodes[0]], isolation, maxPoll)
		if err != nil {
			if errors.Is(err, protocol.ErrRebalanceInProgress) {
				return c.handleFetchRebalance(ctx)
			}
			return nil, err
		}
		return recs, nil
	}
	type nodeFetch struct {
		records []Record
		err     error
	}
	fetches := make([]nodeFetch, len(nodes))
	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)
		go func(i int, node int32) {
			defer wg.Done()
			fetches[i].records, fetches[i].err = c.fetchFromBroker(ctx, group, node, byNode[node], isolation, maxPoll)
		}(i, node)
	}
	wg.Wait()
	var out []Record
	for _, f := range fetches {
		if f.err != nil {
			if errors.Is(f.err, protocol.ErrRebalanceInProgress) {
				return c.handleFetchRebalance(ctx)
			}
			return nil, f.err
		}
		out = append(out, f.records...)
		if len(out) >= maxPoll {
			return out[:maxPoll], nil
		}
	}
	return out, nil
}

func (c *Consumer) handleFetchRebalance(ctx context.Context) ([]Record, error) {
	if c.isCooperative() {
		if err := c.cooperativeRejoin(ctx); err != nil {
			return nil, err
		}
	} else if err := c.Rebalance(ctx); err != nil {
		return nil, err
	}
	return c.Poll(ctx)
}

func (c *Consumer) fetchFromBroker(
	ctx context.Context,
	group string,
	node int32,
	parts []protocol.FetchPartition,
	isolation int8,
	maxRecords int,
) ([]Record, error) {
	body := protocol.EncodeFetchRequest(group, parts, 500, 1, 50<<20, isolation)
	rb, err := c.client.cluster.Request(ctx, node, protocol.APIFetch, protocol.VerFetch, body)
	if err != nil {
		c.client.observe.Metrics.OnConsume(0, err)
		return nil, err
	}
	fetched, err := protocol.DecodeFetchResponse(rb)
	if err != nil {
		return nil, err
	}
	var out []Record
	for _, fr := range fetched {
		out = append(out, Record{
			Topic: fr.Topic, Partition: fr.Partition, Offset: fr.Offset,
			Key: fr.Key, Value: fr.Value, Headers: fetchHeaders(fr.Headers),
			Timestamp: time.UnixMilli(fr.Timestamp),
		})
		c.bumpOffset(fr.Topic, fr.Partition, fr.Offset+1)
		c.client.observe.Metrics.OnConsume(len(fr.Value), nil)
		if maxRecords > 0 && len(out) >= maxRecords {
			break
		}
	}
	return out, nil
}

// GroupMetadata returns the consumer's group generation and member identity for transactional offset commit.
func (c *Consumer) GroupMetadata() (generation int32, memberID, groupInstanceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation, c.memberID, c.client.cfg.Consumer.GroupInstanceID
}

func (c *Consumer) bumpOffset(topic string, part int32, off int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.assignments {
		if c.assignments[i].topic == topic && c.assignments[i].partition == part {
			c.assignments[i].offset = off
			return
		}
	}
}

// Commit commits consumed offsets to the consumer group coordinator.
func (c *Consumer) Commit(ctx context.Context, records ...Record) error {
	return c.commitOffsets(ctx, records, 0)
}

func (c *Consumer) commitOffsets(ctx context.Context, records []Record, attempt int) error {
	const maxCommitAttempts = 20
	if err := c.client.requireOpen(); err != nil {
		return err
	}
	offsets := map[string]map[int32]int64{}
	if len(records) == 0 {
		c.mu.Lock()
		for _, a := range c.assignments {
			if offsets[a.topic] == nil {
				offsets[a.topic] = map[int32]int64{}
			}
			offsets[a.topic][a.partition] = a.offset
		}
		c.mu.Unlock()
	} else {
		for _, r := range records {
			if offsets[r.Topic] == nil {
				offsets[r.Topic] = map[int32]int64{}
			}
			offsets[r.Topic][r.Partition] = r.Offset + 1
		}
	}

	coord, err := c.coordinator(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	group := c.group
	memberID := c.memberID
	generation := c.generation
	instanceID := c.client.cfg.Consumer.GroupInstanceID
	c.mu.Unlock()

	ver := c.client.cluster.NegotiatedVersion(protocol.APIOffsetCommit, protocol.VerOffsetCommit)
	body := protocol.EncodeOffsetCommitRequest(ver, group, memberID, instanceID, generation, offsets)
	rb, err := c.client.cluster.Request(ctx, coord, protocol.APIOffsetCommit, ver, body)
	if err != nil {
		return err
	}
	code, err := protocol.DecodeOffsetCommitResponse(ver, rb)
	if err != nil {
		return err
	}
	if code != 0 {
		if code == int16(ErrCodeRebalanceInProg) && attempt+1 < maxCommitAttempts {
			time.Sleep(500 * time.Millisecond)
			return c.commitOffsets(ctx, records, attempt+1)
		}
		if c.shouldRejoin(newKafkaError(code, "", 0, "offset commit failed")) {
			if attempt+1 < maxCommitAttempts {
				time.Sleep(200 * time.Millisecond)
				if err := c.rejoin(ctx); err != nil {
					return err
				}
				return c.commitOffsets(ctx, records, attempt+1)
			}
		}
		if protocol.CoordinatorRetriable(code) {
			c.invalidateCoordinator()
		}
		return newKafkaError(code, "", 0, "offset commit failed")
	}
	return nil
}

func (c *Consumer) joinAndAssign(ctx context.Context) error {
	if c.useNextGenGroup() {
		return c.joinAndAssign848(ctx)
	}
	if err := c.client.cluster.Refresh(ctx, c.topics); err != nil {
		return err
	}
	coord, err := c.coordinator(ctx)
	if err != nil {
		return err
	}

	assignor := c.client.cfg.Consumer.Assignor.protocolName()
	sessionMs := int32(45000)
	if c.client.cfg.Consumer.SessionTimeout > 0 {
		sessionMs = int32(c.client.cfg.Consumer.SessionTimeout / time.Millisecond)
	}
	rebalanceMs := sessionMs
	if c.client.cfg.Consumer.RebalanceTimeout > 0 {
		rebalanceMs = int32(c.client.cfg.Consumer.RebalanceTimeout / time.Millisecond)
	}

	var joined protocol.JoinGroupResponse
	var assignmentBytes []byte
joinLoop:
	for attempt := 0; attempt < 20; attempt++ {
	joinInner:
		for {
			joinBody := protocol.EncodeJoinGroupRequest(
				c.group, c.memberID, assignor, c.client.cfg.Consumer.GroupInstanceID,
				c.topics, sessionMs, rebalanceMs, c.isCooperative(),
			)
			rb, err := c.client.cluster.Request(ctx, coord, protocol.APIJoinGroup, protocol.VerJoinGroup, joinBody)
			if err != nil {
				return err
			}
			joined, err = protocol.DecodeJoinGroupResponse(rb)
			if errors.Is(err, protocol.ErrMemberIDRequired) {
				c.mu.Lock()
				c.memberID = joined.MemberID
				c.mu.Unlock()
				continue
			}
			if code, ok := protocol.APIErrorCode(err); ok && protocol.CoordinatorRetriable(code) {
				c.client.cluster.Invalidate(coord)
				c.invalidateCoordinator()
				coord, err = c.coordinator(ctx)
				if err != nil {
					return err
				}
				continue joinLoop
			}
			if err != nil {
				return err
			}
			break joinInner
		}

		c.mu.Lock()
		c.memberID = joined.MemberID
		c.generation = joined.GenerationID
		c.mu.Unlock()

		syncAssignments := map[string][]byte{joined.MemberID: {}}
		if joined.MemberID == joined.LeaderID {
			var members []protocol.MemberSubscription
			for mid, meta := range joined.Assignments {
				topics, err := protocol.DecodeConsumerSubscription(meta)
				if err != nil {
					return err
				}
				members = append(members, protocol.MemberSubscription{MemberID: mid, Topics: topics})
			}
			if len(members) > 0 {
				meta := c.client.cluster.Metadata()
				topicParts := map[string][]int32{}
				for _, t := range meta.Topics {
					parts := make([]int32, 0, len(t.Partitions))
					for _, p := range t.Partitions {
						parts = append(parts, p.Partition)
					}
					topicParts[t.Name] = parts
				}
				syncAssignments = protocol.ComputeGroupAssignments(joined.Protocol, members, topicParts)
			}
		}

		syncBody := protocol.EncodeSyncGroupRequest(c.group, joined.MemberID, joined.Protocol, joined.GenerationID, syncAssignments)
		rb, err := c.client.cluster.Request(ctx, coord, protocol.APISyncGroup, protocol.VerSyncGroup, syncBody)
		if err != nil {
			return err
		}
		assignmentBytes, err = protocol.DecodeSyncGroupResponse(rb)
		if err != nil {
			if c.shouldRejoin(err) {
				c.invalidateCoordinator()
				coord, err = c.coordinator(ctx)
				if err != nil {
					return err
				}
				continue joinLoop
			}
			return err
		}
		break joinLoop
	}

	listenersNotified, err := c.applyAssignment(ctx, assignmentBytes, coord)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.coordID = coord
	c.hasCoord = true
	c.mu.Unlock()
	if err := c.loadCommittedOffsets(ctx, coord); err != nil {
		return err
	}
	if !listenersNotified {
		c.mu.Lock()
		c.notifyAssignedLocked(ctx)
		c.mu.Unlock()
	}
	c.ensureHeartbeat()
	_ = c.heartbeat(ctx)
	return nil
}

func (c *Consumer) applyAssignment(ctx context.Context, raw []byte, _ int32) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	parsed, err := protocol.ParseMemberAssignment(raw)
	if err != nil {
		return false, err
	}
	if c.isCooperative() && len(parsed) > 0 {
		c.applyAssignmentIncrementalLocked(ctx, parsed)
		return true, nil
	}
	c.notifyRevokedLocked(ctx)
	if len(parsed) > 0 {
		c.assignments = nil
		for _, a := range parsed {
			for _, p := range a.Partitions {
				c.assignments = append(c.assignments, partitionOffset{
					topic: a.Topic, partition: p, offset: 0,
				})
			}
		}
		return false, nil
	}
	if c.isCooperative() {
		c.assignments = nil
		return false, nil
	}
	// Fallback for single-member dev clusters when coordinator returns empty assignment.
	c.assignments = nil
	meta := c.client.cluster.Metadata()
	for _, topic := range c.topics {
		for _, t := range meta.Topics {
			if t.Name != topic {
				continue
			}
			for _, p := range t.Partitions {
				c.assignments = append(c.assignments, partitionOffset{
					topic: topic, partition: p.Partition, offset: 0,
				})
			}
		}
	}
	return false, nil
}

func (c *Consumer) applyAssignmentIncrementalLocked(ctx context.Context, parsed []protocol.TopicPartitionAssignment) {
	newSet := make(map[partKey]struct{})
	for _, a := range parsed {
		for _, p := range a.Partitions {
			newSet[partKey{a.Topic, p}] = struct{}{}
		}
	}
	kept := c.assignments[:0]
	var revoked []TopicPartition
	for _, a := range c.assignments {
		k := partKey{a.topic, a.partition}
		if _, ok := newSet[k]; ok {
			kept = append(kept, a)
		} else {
			revoked = append(revoked, TopicPartition{Topic: a.topic, Partition: a.partition, Offset: a.offset})
		}
	}
	if c.listener != nil && len(revoked) > 0 {
		c.listener.OnPartitionsRevoked(ctx, revoked)
	}
	existing := make(map[partKey]struct{}, len(kept))
	for _, a := range kept {
		existing[partKey{a.topic, a.partition}] = struct{}{}
	}
	var assigned []TopicPartition
	for _, a := range parsed {
		for _, p := range a.Partitions {
			k := partKey{a.Topic, p}
			if _, ok := existing[k]; ok {
				continue
			}
			kept = append(kept, partitionOffset{topic: a.Topic, partition: p, offset: 0})
			assigned = append(assigned, TopicPartition{Topic: a.Topic, Partition: p})
		}
	}
	c.assignments = kept
	if c.listener != nil && len(assigned) > 0 {
		c.listener.OnPartitionsAssigned(ctx, assigned)
	}
}

func (c *Consumer) loadCommittedOffsets(ctx context.Context, coord int32) error {
	c.mu.Lock()
	assignments := append([]partitionOffset(nil), c.assignments...)
	group := c.group
	memberID := c.memberID
	c.mu.Unlock()
	if len(assignments) == 0 {
		return nil
	}
	parts := make([]protocol.OffsetFetchPartition, len(assignments))
	for i, a := range assignments {
		parts[i] = protocol.OffsetFetchPartition{Topic: a.topic, Partition: a.partition}
	}
	body := protocol.EncodeOffsetFetchRequest(group, memberID, parts)
	rb, err := c.client.cluster.Request(ctx, coord, protocol.APIOffsetFetch, protocol.VerOffsetFetch, body)
	if err != nil {
		return err
	}
	committed, err := protocol.DecodeOffsetFetchResponse(rb)
	if err != nil {
		return err
	}
	for _, co := range committed {
		if co.ErrorCode != 0 {
			continue
		}
		if co.Offset >= 0 {
			c.bumpOffset(co.Topic, co.Partition, co.Offset)
		} else if c.client.cfg.Consumer.ConsumeFromBeginning {
			if c.client.cfg.Consumer.IsolationLevel == IsolationReadCommitted {
				if err := c.Seek(co.Topic, co.Partition, 0); err != nil {
					return err
				}
			} else if err := c.SeekToBeginning(ctx, co.Topic, co.Partition); err != nil {
				return err
			}
		} else {
			if err := c.SeekToEnd(ctx, co.Topic, co.Partition); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Consumer) ensureHeartbeat() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hbCancel != nil {
		return
	}
	hbCtx, cancel := context.WithCancel(context.Background())
	c.hbCancel = cancel
	go c.heartbeatLoop(hbCtx)
}

func (c *Consumer) stopHeartbeat() {
	c.mu.Lock()
	cancel := c.hbCancel
	c.hbCancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Consumer) coordinator(ctx context.Context) (int32, error) {
	c.mu.Lock()
	if c.hasCoord {
		id := c.coordID
		c.mu.Unlock()
		return id, nil
	}
	group := c.group
	if group == "" {
		group = c.client.cfg.ConsumerGroup
	}
	c.mu.Unlock()

	id, err := c.client.cluster.FindCoordinator(ctx, group, protocol.CoordinatorGroup)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.coordID = id
	c.hasCoord = true
	c.mu.Unlock()
	return id, nil
}

func (c *Consumer) invalidateCoordinator() {
	c.mu.Lock()
	c.hasCoord = false
	c.mu.Unlock()
}

func (c *Consumer) shouldRejoin(err error) bool {
	code, ok := protocol.APIErrorCode(err)
	if !ok {
		var ke *KafkaError
		if errors.As(err, &ke) {
			code = int16(ke.Code)
			ok = true
		}
	}
	if !ok {
		return false
	}
	switch code {
	case int16(ErrCodeRebalanceInProg), int16(ErrCodeNotCoordinator),
		25, 22: // UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION
		return true
	default:
		return protocol.CoordinatorRetriable(code)
	}
}

func (c *Consumer) rejoin(ctx context.Context) error {
	c.mu.Lock()
	c.assignments = nil
	c.hasCoord = false
	c.mu.Unlock()
	return c.joinAndAssign(ctx)
}

func (c *Consumer) isCooperative() bool {
	return c.client.cfg.Consumer.Assignor == AssignorCooperativeSticky
}

func (c *Consumer) cooperativeRejoin(ctx context.Context) error {
	c.notifyRevoked(ctx)
	return c.joinAndAssign(ctx)
}

func fetchHeaders(h [][2][]byte) []Header {
	if len(h) == 0 {
		return nil
	}
	out := make([]Header, len(h))
	for i, pair := range h {
		out[i] = Header{Key: string(pair[0]), Value: pair[1]}
	}
	return out
}
