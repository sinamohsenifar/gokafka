package gokafka

import (
	"context"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// PartitionReassignmentResult is the per-partition outcome of
// AlterPartitionReassignments. Err is nil on success.
type PartitionReassignmentResult struct {
	Topic     string
	Partition int32
	Err       error
}

// OngoingPartitionReassignment describes an in-progress reassignment: the
// current replica set plus the replicas being added and removed.
type OngoingPartitionReassignment struct {
	Topic            string
	Partition        int32
	Replicas         []int32
	AddingReplicas   []int32
	RemovingReplicas []int32
}

// AlterPartitionReassignments moves partition replicas to new broker sets
// (KIP-455, API 45). assignments maps topic -> partition -> target replica
// broker ids; a nil replica slice cancels an in-progress reassignment for that
// partition. Returns the per-partition results.
func (a *Admin) AlterPartitionReassignments(ctx context.Context, assignments map[string]map[int32][]int32) ([]PartitionReassignmentResult, error) {
	topics := make(map[string][]protocol.ReassignmentMove, len(assignments))
	for topic, parts := range assignments {
		moves := make([]protocol.ReassignmentMove, 0, len(parts))
		for part, replicas := range parts {
			moves = append(moves, protocol.ReassignmentMove{Partition: part, Replicas: replicas})
		}
		topics[topic] = moves
	}
	body := protocol.EncodeAlterPartitionReassignmentsRequest(30000, topics)
	resp, err := a.requestAny(ctx, protocol.APIAlterPartitionReassign, protocol.VerAlterPartitionReassign, body)
	if err != nil {
		return nil, err
	}
	topErr, topMsg, results, err := protocol.DecodeAlterPartitionReassignmentsResponse(resp)
	if err != nil {
		return nil, err
	}
	if topErr != 0 {
		return nil, newKafkaError(topErr, "", 0, topMsg)
	}
	out := make([]PartitionReassignmentResult, 0, len(results))
	for _, r := range results {
		res := PartitionReassignmentResult{Topic: r.Topic, Partition: r.Partition}
		if r.ErrorCode != 0 {
			res.Err = newKafkaError(r.ErrorCode, r.Topic, r.Partition, r.ErrorMessage)
		}
		out = append(out, res)
	}
	return out, nil
}

// ListPartitionReassignments returns the in-progress partition reassignments
// (KIP-455, API 46). With no topicPartitions it lists all ongoing reassignments
// in the cluster; otherwise it restricts to the given topic partitions.
func (a *Admin) ListPartitionReassignments(ctx context.Context, topicPartitions map[string][]int32) ([]OngoingPartitionReassignment, error) {
	body := protocol.EncodeListPartitionReassignmentsRequest(30000, topicPartitions)
	resp, err := a.requestAny(ctx, protocol.APIListPartitionReassign, protocol.VerListPartitionReassign, body)
	if err != nil {
		return nil, err
	}
	topErr, topMsg, ongoing, err := protocol.DecodeListPartitionReassignmentsResponse(resp)
	if err != nil {
		return nil, err
	}
	if topErr != 0 {
		return nil, newKafkaError(topErr, "", 0, topMsg)
	}
	out := make([]OngoingPartitionReassignment, 0, len(ongoing))
	for _, o := range ongoing {
		out = append(out, OngoingPartitionReassignment{
			Topic: o.Topic, Partition: o.Partition,
			Replicas: o.Replicas, AddingReplicas: o.AddingReplicas, RemovingReplicas: o.RemovingReplicas,
		})
	}
	return out, nil
}
