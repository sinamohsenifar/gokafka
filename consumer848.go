package gokafka

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

func (c *Consumer) useNextGenGroup() bool {
	return c.client.cfg.Consumer.GroupProtocol == GroupProtocolNextGen
}

// newMemberUUID generates a group member id in Kafka's canonical Uuid string
// form: URL-safe base64 of 16 random bytes, no padding (22 chars), matching
// Uuid.randomUuid().toString() in the Java client. This is required by the
// ShareFetch handler (KIP-932), which parses the member id via Uuid.fromString;
// a hyphenated RFC-4122 string is rejected as "too long to be decoded as a
// base64 UUID". The classic KIP-848 path treats it as an opaque key, so the
// same form is valid there too.
func newMemberUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return base64.RawURLEncoding.EncodeToString(b[:])
}

func (c *Consumer) serverAssignor848() string {
	switch c.client.cfg.Consumer.Assignor {
	case AssignorCooperativeSticky:
		return "uniform"
	default:
		return "range"
	}
}

func (c *Consumer) joinAndAssign848(ctx context.Context) error {
	if err := c.client.cluster.Refresh(ctx, c.topics); err != nil {
		return err
	}
	coord, err := c.coordinator(ctx)
	if err != nil {
		return err
	}

	c.mu.Lock()
	if c.memberID == "" {
		c.memberID = newMemberUUID()
	}
	memberID := c.memberID
	c.mu.Unlock()

	rebalanceMs := int32(45000)
	if c.client.cfg.Consumer.RebalanceTimeout > 0 {
		rebalanceMs = int32(c.client.cfg.Consumer.RebalanceTimeout / time.Millisecond)
	}
	if c.client.cfg.Consumer.SessionTimeout > 0 && rebalanceMs <= 0 {
		rebalanceMs = int32(c.client.cfg.Consumer.SessionTimeout / time.Millisecond)
	}

	assignor := c.serverAssignor848()
	var instanceID *string
	if id := c.client.cfg.Consumer.GroupInstanceID; id != "" {
		instanceID = &id
	}
	var gotAssignment bool
	for attempt := 0; attempt < 30; attempt++ {
		c.mu.Lock()
		memberEpoch := c.generation
		c.mu.Unlock()

		req := protocol.ConsumerGroupHeartbeatRequest{
			GroupID:            c.group,
			MemberID:           memberID,
			MemberEpoch:        memberEpoch,
			InstanceID:         instanceID,
			RebalanceTimeoutMs: rebalanceMs,
			ServerAssignor:     &assignor,
		}
		if c.topicRegex != "" {
			req.SubscribedTopicRegex = &c.topicRegex // KIP-848 server-side RE2J
		} else {
			req.SubscribedTopicNames = append([]string(nil), c.topics...)
		}
		if memberEpoch == 0 {
			// Broker requires an empty (non-null) topic partition list when joining.
			req.TopicPartitions = []protocol.TopicIDPartitions{}
		}
		if memberEpoch > 0 {
			req.SubscribedTopicNames = nil
			req.SubscribedTopicRegex = nil
			req.ServerAssignor = nil
			req.InstanceID = nil
			req.RebalanceTimeoutMs = -1
			req.TopicPartitions = c.ownedTopicPartitions848()
		}

		resp, err := c.sendGroupHeartbeat848(ctx, coord, req)
		if err != nil {
			if c.shouldRejoin848(err) {
				c.mu.Lock()
				c.generation = 0
				c.mu.Unlock()
				c.invalidateCoordinator()
				coord, err = c.coordinator(ctx)
				if err != nil {
					return err
				}
				continue
			}
			if protocol.CoordinatorRetriable(protocolErrorCode(err)) {
				c.invalidateCoordinator()
				coord, err = c.coordinator(ctx)
				if err != nil {
					return err
				}
				continue
			}
			return err
		}

		c.mu.Lock()
		c.memberID = memberID
		if resp.MemberID != "" {
			c.memberID = resp.MemberID
		}
		c.generation = resp.MemberEpoch
		c.coordID = coord
		c.hasCoord = true
		c.mu.Unlock()

		if resp.HeartbeatIntervalMs > 0 {
			c.client.cfg.Consumer.HeartbeatInterval = time.Duration(resp.HeartbeatIntervalMs) * time.Millisecond
		}

		if len(resp.Assignment) > 0 {
			raw, err := c.assignment848ToMemberBytes(resp.Assignment)
			if err != nil {
				return err
			}
			listenersNotified, err := c.applyAssignment(ctx, raw, coord)
			if err != nil {
				return err
			}
			if err := c.loadCommittedOffsets(ctx, coord); err != nil {
				return err
			}
			if !listenersNotified {
				c.mu.Lock()
				c.notifyAssignedLocked(ctx)
				c.mu.Unlock()
			}
			gotAssignment = true
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !gotAssignment {
		return fmt.Errorf("gokafka: consumer group heartbeat join: no partition assignment")
	}
	c.ensureHeartbeat()
	return c.heartbeat848(ctx)
}

func (c *Consumer) sendGroupHeartbeat848(ctx context.Context, coord int32, req protocol.ConsumerGroupHeartbeatRequest) (protocol.ConsumerGroupHeartbeatResponse, error) {
	ver := c.client.cluster.NegotiatedVersion(protocol.APIConsumerGroupHeartbeat, protocol.VerConsumerGroupHeartbeat)
	body := protocol.EncodeConsumerGroupHeartbeatRequest(ver, req)
	rb, err := c.client.cluster.Request(ctx, coord, protocol.APIConsumerGroupHeartbeat, ver, body)
	if err != nil {
		return protocol.ConsumerGroupHeartbeatResponse{}, err
	}
	resp, err := protocol.DecodeConsumerGroupHeartbeatResponse(rb)
	if err != nil {
		if legacy, legErr := protocol.DecodeConsumerGroupHeartbeatResponseLegacy(rb); legErr == nil {
			return legacy, nil
		}
		return protocol.ConsumerGroupHeartbeatResponse{}, err
	}
	return resp, nil
}

func (c *Consumer) ownedTopicPartitions848() []protocol.TopicIDPartitions {
	c.mu.Lock()
	assigns := append([]partitionOffset(nil), c.assignments...)
	c.mu.Unlock()
	byTopic := map[wire.UUID][]int32{}
	for _, a := range assigns {
		id, ok := c.client.cluster.TopicIDByName(a.topic)
		if !ok {
			continue
		}
		byTopic[id] = append(byTopic[id], a.partition)
	}
	out := make([]protocol.TopicIDPartitions, 0, len(byTopic))
	for id, parts := range byTopic {
		out = append(out, protocol.TopicIDPartitions{TopicID: id, Partitions: parts})
	}
	return out
}

func (c *Consumer) assignment848ToMemberBytes(assign []protocol.TopicIDPartitions) ([]byte, error) {
	var parsed []protocol.TopicPartitionAssignment
	for _, tp := range assign {
		name, ok := c.client.cluster.TopicNameByID(tp.TopicID)
		if !ok {
			return nil, fmt.Errorf("gokafka: unknown topic id in assignment")
		}
		parsed = append(parsed, protocol.TopicPartitionAssignment{Topic: name, Partitions: tp.Partitions})
	}
	return protocol.EncodeMemberAssignment(parsed), nil
}

func (c *Consumer) heartbeat848(ctx context.Context) error {
	c.mu.Lock()
	memberID := c.memberID
	epoch := c.generation
	group := c.group
	c.mu.Unlock()
	if memberID == "" {
		return nil
	}
	coord, err := c.coordinator(ctx)
	if err != nil {
		return err
	}
	req := protocol.ConsumerGroupHeartbeatRequest{
		GroupID:         group,
		MemberID:        memberID,
		MemberEpoch:     epoch,
		TopicPartitions: c.ownedTopicPartitions848(),
	}
	_, err = c.sendGroupHeartbeat848(ctx, coord, req)
	if err != nil && c.shouldRejoin848(err) {
		return c.rejoin848(ctx)
	}
	return err
}

func (c *Consumer) rejoin848(ctx context.Context) error {
	c.mu.Lock()
	c.assignments = nil
	c.generation = 0
	c.hasCoord = false
	c.mu.Unlock()
	return c.joinAndAssign848(ctx)
}

func (c *Consumer) shouldRejoin848(err error) bool {
	code := protocolErrorCode(err)
	switch code {
	case 25, 110, 112: // UNKNOWN_MEMBER_ID, FENCED_MEMBER_EPOCH, STALE_MEMBER_EPOCH
		return true
	default:
		return false
	}
}

func protocolErrorCode(err error) int16 {
	if code, ok := protocol.APIErrorCode(err); ok {
		return code
	}
	var ke *KafkaError
	if errors.As(err, &ke) {
		return int16(ke.Code)
	}
	return 0
}

func (c *Consumer) leave848(ctx context.Context) error {
	c.mu.Lock()
	memberID := c.memberID
	group := c.group
	c.mu.Unlock()
	if memberID == "" {
		return nil
	}
	coord, err := c.coordinator(ctx)
	if err != nil {
		return err
	}
	req := protocol.ConsumerGroupHeartbeatRequest{
		GroupID:     group,
		MemberID:    memberID,
		MemberEpoch: -1,
	}
	_, err = c.sendGroupHeartbeat848(ctx, coord, req)
	return err
}
