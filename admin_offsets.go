package gokafka

import (
	"context"
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// CommittedOffset is a consumer group's committed offset for a topic partition.
type CommittedOffset struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
	ErrorCode int16
}

// FetchOffsets returns the committed offsets for one or more consumer groups
// using the batched OffsetFetch API (KIP-709, v8+): groups sharing a coordinator
// are fetched in a single request. The result maps each group id to all of its
// committed offsets. Requires a broker that supports OffsetFetch v8+ (Kafka 3.0+).
func (a *Admin) FetchOffsets(ctx context.Context, groups ...string) (map[string][]CommittedOffset, error) {
	if len(groups) == 0 {
		return map[string][]CommittedOffset{}, nil
	}
	neg := a.client.cluster.NegotiatedVersion(protocol.APIOffsetFetch, protocol.VerOffsetFetch)
	if neg < protocol.VerOffsetFetchMultiGroup {
		return nil, fmt.Errorf("gokafka: batched OffsetFetch requires broker OffsetFetch v8+ (negotiated v%d)", neg)
	}
	ver := protocol.VerOffsetFetchMultiGroup

	// Different groups may be hosted by different coordinators; a batched request
	// must go to the right one, so group the requested ids by coordinator node.
	byCoord := map[int32][]string{}
	for _, g := range groups {
		if g == "" {
			return nil, ErrNoConsumerGroup
		}
		coord, err := a.client.cluster.FindCoordinator(ctx, g, protocol.CoordinatorGroup)
		if err != nil {
			return nil, err
		}
		byCoord[coord] = append(byCoord[coord], g)
	}

	out := make(map[string][]CommittedOffset, len(groups))
	for coord, gs := range byCoord {
		partitionsByGroup := make(map[string][]protocol.OffsetFetchPartition, len(gs))
		for _, g := range gs {
			partitionsByGroup[g] = nil // nil = all topics for the group
		}
		body := protocol.EncodeOffsetFetchMultiGroupRequest(ver, partitionsByGroup)
		resp, err := a.client.cluster.Request(ctx, coord, protocol.APIOffsetFetch, ver, body)
		if err != nil {
			return nil, err
		}
		decoded, err := protocol.DecodeOffsetFetchMultiGroupResponse(ver, resp)
		if err != nil {
			return nil, err
		}
		for g, offs := range decoded {
			committed := make([]CommittedOffset, 0, len(offs))
			for _, o := range offs {
				committed = append(committed, CommittedOffset{
					Topic: o.Topic, Partition: o.Partition, Offset: o.Offset,
					Metadata: o.Metadata, ErrorCode: o.ErrorCode,
				})
			}
			out[g] = committed
		}
	}
	return out, nil
}
