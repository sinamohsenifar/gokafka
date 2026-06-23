package gokafka

import (
	"context"
)

// RebalanceListener receives partition assignment lifecycle events (Java ConsumerRebalanceListener equivalent).
type RebalanceListener interface {
	// OnPartitionsRevoked is called before partitions are reassigned away from this consumer.
	OnPartitionsRevoked(ctx context.Context, partitions []TopicPartition)
	// OnPartitionsAssigned is called after new partitions are assigned to this consumer.
	OnPartitionsAssigned(ctx context.Context, partitions []TopicPartition)
}

// RebalanceFunc adapts functions to RebalanceListener.
type RebalanceFunc struct {
	Revoked  func(ctx context.Context, partitions []TopicPartition)
	Assigned func(ctx context.Context, partitions []TopicPartition)
}

func (f RebalanceFunc) OnPartitionsRevoked(ctx context.Context, parts []TopicPartition) {
	if f.Revoked != nil {
		f.Revoked(ctx, parts)
	}
}

func (f RebalanceFunc) OnPartitionsAssigned(ctx context.Context, parts []TopicPartition) {
	if f.Assigned != nil {
		f.Assigned(ctx, parts)
	}
}

// SetRebalanceListener registers callbacks for group rebalance events.
func (c *Consumer) SetRebalanceListener(l RebalanceListener) {
	c.listener = l
}

func (c *Consumer) notifyRevoked(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notifyRevokedLocked(ctx)
}

func (c *Consumer) notifyAssigned(ctx context.Context) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.notifyAssignedLocked(ctx)
}

func (c *Consumer) notifyRevokedLocked(ctx context.Context) {
	if c.listener == nil || len(c.assignments) == 0 {
		return
	}
	c.listener.OnPartitionsRevoked(ctx, c.assignedPartitionsLocked())
}

func (c *Consumer) notifyAssignedLocked(ctx context.Context) {
	if c.listener == nil {
		return
	}
	c.listener.OnPartitionsAssigned(ctx, c.assignedPartitionsLocked())
}

func (c *Consumer) assignedPartitionsLocked() []TopicPartition {
	out := make([]TopicPartition, len(c.assignments))
	for i, a := range c.assignments {
		out[i] = TopicPartition{Topic: a.topic, Partition: a.partition, Offset: a.offset}
	}
	return out
}

// Rebalance triggers re-join to pick up new group assignments.
// Cooperative assignors rejoin without LeaveGroup; eager assignors leave first.
func (c *Consumer) Rebalance(ctx context.Context) error {
	if err := c.client.requireOpen(); err != nil {
		return err
	}
	if c.client.cfg.ConsumerGroup == "" {
		return ErrNoConsumerGroup
	}
	c.mu.Lock()
	c.group = c.client.cfg.ConsumerGroup
	c.notifyRevokedLocked(ctx)
	c.mu.Unlock()
	if c.isCooperative() {
		return c.joinAndAssign(ctx)
	}
	if err := c.Leave(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	c.assignments = nil
	c.memberID = ""
	c.generation = 0
	c.hasCoord = false
	c.mu.Unlock()
	return c.joinAndAssign(ctx)
}
