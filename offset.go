package gokafka

import (
	"context"
	"fmt"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// Seek moves consumption to an explicit offset for a subscribed partition.
// Not locked under c.mu on purpose: rebalance listeners (OnPartitionsAssigned)
// run while the consumer holds c.mu and commonly Seek from there, so taking the
// lock here would deadlock. Poll's advanceDelivered compare-and-set (it only
// advances a partition still at the offset it fetched from) keeps a concurrent
// Seek from being clobbered.
func (c *Consumer) Seek(topic string, partition int32, offset int64) error {
	if err := c.client.requireOpen(); err != nil {
		return err
	}
	for i := range c.assignments {
		if c.assignments[i].topic == topic && c.assignments[i].partition == partition {
			c.assignments[i].offset = offset
			return nil
		}
	}
	return fmt.Errorf("gokafka: partition %s-%d not assigned", topic, partition)
}

// SeekToBeginning resolves earliest offsets via ListOffsets and seeks there.
func (c *Consumer) SeekToBeginning(ctx context.Context, topic string, partitions ...int32) error {
	return c.seekByTimestamp(ctx, topic, -2, partitions...)
}

// SeekToEnd resolves latest offsets via ListOffsets and seeks there.
func (c *Consumer) SeekToEnd(ctx context.Context, topic string, partitions ...int32) error {
	return c.seekByTimestamp(ctx, topic, -1, partitions...)
}

// SeekToTime resolves, per partition, the earliest offset whose timestamp is at
// or after t (via ListOffsets) and seeks there.
func (c *Consumer) SeekToTime(ctx context.Context, topic string, t time.Time, partitions ...int32) error {
	return c.seekByTimestamp(ctx, topic, t.UnixMilli(), partitions...)
}

func (c *Consumer) seekByTimestamp(ctx context.Context, topic string, ts int64, partitions ...int32) error {
	if len(partitions) == 0 {
		for _, a := range c.assignments {
			if a.topic == topic {
				partitions = append(partitions, a.partition)
			}
		}
	}
	if len(partitions) == 0 {
		return fmt.Errorf("gokafka: no partitions for topic %s", topic)
	}
	req := make([]protocol.ListOffsetsPartition, len(partitions))
	for i, p := range partitions {
		req[i] = protocol.ListOffsetsPartition{Topic: topic, Partition: p, Timestamp: ts}
	}
	isolation := int8(0)
	if c.client.cfg.Consumer.IsolationLevel == IsolationReadCommitted {
		isolation = 1
	}
	body := protocol.EncodeListOffsetsRequest(req, isolation)
	// A just-created topic's partition leader may not be elected/propagated yet,
	// yielding NOT_LEADER_OR_FOLLOWER / LEADER_NOT_AVAILABLE. Retry patiently with
	// a metadata refresh until the leader is ready (bounded by ctx).
	return retryRetriable(ctx, coordinatorRetry(c.client.cfg.Retry), func() error {
		leader, err := c.client.cluster.LeaderBroker(topic, partitions[0])
		if err != nil {
			_ = c.client.cluster.Refresh(ctx, []string{topic})
			return newKafkaError(int16(ErrCodeNotLeaderForPart), topic, partitions[0], "list offsets: leader unavailable")
		}
		rb, err := c.client.cluster.Request(ctx, leader.NodeID, protocol.APIListOffsets, protocol.VerListOffsets, body)
		if err != nil {
			return err
		}
		offs, err := protocol.DecodeListOffsetsResponse(rb)
		if err != nil {
			return err
		}
		for _, o := range offs {
			if o.ErrorCode != 0 {
				ke := newKafkaError(o.ErrorCode, o.Topic, o.Partition, "list offsets failed")
				if IsRetriable(ke) {
					_ = c.client.cluster.Refresh(ctx, []string{topic})
				}
				return ke
			}
			if err := c.Seek(o.Topic, o.Partition, o.Offset); err != nil {
				return err
			}
		}
		return nil
	})
}

// AssignedPartitions returns the current group assignment.
func (c *Consumer) AssignedPartitions() []TopicPartition {
	out := make([]TopicPartition, len(c.assignments))
	for i, a := range c.assignments {
		out[i] = TopicPartition{Topic: a.topic, Partition: a.partition, Offset: a.offset}
	}
	return out
}

// TopicPartition identifies a topic partition and optional current offset.
type TopicPartition struct {
	Topic     string
	Partition int32
	Offset    int64
}
