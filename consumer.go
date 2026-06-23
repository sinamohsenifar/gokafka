package gokafka

import (
	"context"
	"errors"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/observe"
)

// Consumer reads from topics using a consumer group.
type Consumer struct {
	client      *Client
	topics      []string
	group       string
	memberID    string
	generation  int32
	assignments []partitionOffset
	listener    RebalanceListener
	paused      map[partKey]struct{}
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
	c.group = c.client.cfg.ConsumerGroup

	if len(c.assignments) == 0 {
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
	for _, a := range c.assignments {
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

	var out []Record
	for node, parts := range byNode {
		body := protocol.EncodeFetchRequest(c.group, parts, 500, 1, 50<<20, isolation)
		rb, err := c.client.cluster.Request(ctx, node, protocol.APIFetch, protocol.VerFetch, body)
		if err != nil {
			c.client.observe.Metrics.OnConsume(0, err)
			return nil, err
		}
		fetched, err := protocol.DecodeFetchResponse(rb)
		if err != nil {
			if errors.Is(err, protocol.ErrRebalanceInProgress) {
				if c.isCooperative() {
					if rbErr := c.cooperativeRejoin(ctx); rbErr != nil {
						return nil, rbErr
					}
				} else if rbErr := c.Rebalance(ctx); rbErr != nil {
					return nil, rbErr
				}
				return c.Poll(ctx)
			}
			return nil, err
		}
		for _, fr := range fetched {
			out = append(out, Record{
				Topic: fr.Topic, Partition: fr.Partition, Offset: fr.Offset,
				Key: fr.Key, Value: fr.Value, Headers: fetchHeaders(fr.Headers),
				Timestamp: time.UnixMilli(fr.Timestamp),
			})
			c.bumpOffset(fr.Topic, fr.Partition, fr.Offset+1)
			c.client.observe.Metrics.OnConsume(len(fr.Value), nil)
			if len(out) >= maxPoll {
				return out, nil
			}
		}
	}
	return out, nil
}

func (c *Consumer) bumpOffset(topic string, part int32, off int64) {
	for i := range c.assignments {
		if c.assignments[i].topic == topic && c.assignments[i].partition == part {
			c.assignments[i].offset = off
			return
		}
	}
}

// Commit commits consumed offsets to the consumer group coordinator.
func (c *Consumer) Commit(ctx context.Context, records ...Record) error {
	if err := c.client.requireOpen(); err != nil {
		return err
	}
	offsets := map[string]map[int32]int64{}
	for _, r := range records {
		if offsets[r.Topic] == nil {
			offsets[r.Topic] = map[int32]int64{}
		}
		offsets[r.Topic][r.Partition] = r.Offset + 1
	}
	for _, a := range c.assignments {
		if offsets[a.topic] == nil {
			offsets[a.topic] = map[int32]int64{}
		}
		if _, ok := offsets[a.topic][a.partition]; !ok {
			offsets[a.topic][a.partition] = a.offset
		}
	}

	coord, err := c.coordinator(ctx)
	if err != nil {
		return err
	}
	body := protocol.EncodeOffsetCommitRequest(c.group, c.memberID, c.client.cfg.Consumer.GroupInstanceID, c.generation, offsets)
	_, err = c.client.cluster.Request(ctx, coord, protocol.APIOffsetCommit, protocol.VerOffsetCommit, body)
	return err
}

func (c *Consumer) joinAndAssign(ctx context.Context) error {
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
joinLoop:
	for attempt := 0; attempt < 20; attempt++ {
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
				c.memberID = joined.MemberID
				continue
			}
			if code, ok := protocol.APIErrorCode(err); ok && protocol.CoordinatorRetriable(code) {
				c.client.cluster.Invalidate(coord)
				coord, err = c.coordinator(ctx)
				if err != nil {
					return err
				}
				continue joinLoop
			}
			if err != nil {
				return err
			}
			break joinLoop
		}
	}
	c.memberID = joined.MemberID
	c.generation = joined.GenerationID

	assignments := map[string][]byte{joined.MemberID: {}}
	syncBody := protocol.EncodeSyncGroupRequest(c.group, joined.MemberID, joined.Protocol, joined.GenerationID, assignments)
	rb, err := c.client.cluster.Request(ctx, coord, protocol.APISyncGroup, protocol.VerSyncGroup, syncBody)
	if err != nil {
		return err
	}
	assignmentBytes, err := protocol.DecodeSyncGroupResponse(rb)
	if err != nil {
		return err
	}

	if err := c.applyAssignment(ctx, assignmentBytes, coord); err != nil {
		return err
	}
	if err := c.loadCommittedOffsets(ctx, coord); err != nil {
		return err
	}
	c.notifyAssigned(ctx)
	return nil
}

func (c *Consumer) applyAssignment(ctx context.Context, raw []byte, _ int32) error {
	c.notifyRevoked(ctx)
	parsed, err := protocol.ParseMemberAssignment(raw)
	if err != nil {
		return err
	}
	if len(parsed) > 0 {
		c.assignments = nil
		for _, a := range parsed {
			for _, p := range a.Partitions {
				c.assignments = append(c.assignments, partitionOffset{
					topic: a.Topic, partition: p, offset: 0,
				})
			}
		}
		return nil
	}
	if c.isCooperative() {
		c.assignments = nil
		return nil
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
	return nil
}

func (c *Consumer) loadCommittedOffsets(ctx context.Context, coord int32) error {
	if len(c.assignments) == 0 {
		return nil
	}
	parts := make([]protocol.OffsetFetchPartition, len(c.assignments))
	for i, a := range c.assignments {
		parts[i] = protocol.OffsetFetchPartition{Topic: a.topic, Partition: a.partition}
	}
	body := protocol.EncodeOffsetFetchRequest(c.group, c.memberID, parts)
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

func (c *Consumer) coordinator(ctx context.Context) (int32, error) {
	return c.client.cluster.FindCoordinator(ctx, c.group, protocol.CoordinatorGroup)
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
