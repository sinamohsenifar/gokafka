package gokafka

import (
	"context"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// PartitionLag is a consumer group's lag for a single partition: how far the
// committed offset trails the partition's log-end (latest) offset.
type PartitionLag struct {
	Topic        string
	Partition    int32
	Committed    int64 // committed group offset (-1 if the group has no commit)
	LogEndOffset int64 // partition high watermark (next offset to be produced)
	Lag          int64 // LogEndOffset - Committed (0 when there is no commit)
}

// ConsumerGroupLag returns per-partition lag for a consumer group — the gap
// between each partition's log-end offset and the group's committed offset. It
// fetches the group's committed offsets (OffsetFetch) and the partitions'
// latest offsets (ListOffsets) and pairs them. Returns an empty slice if the
// group has no committed offsets. This is the common lag-monitoring primitive
// that franz-go's kadm exposes; here it is built from the protocol directly.
func (a *Admin) ConsumerGroupLag(ctx context.Context, group string) ([]PartitionLag, error) {
	if group == "" {
		return nil, ErrNoConsumerGroup
	}
	byGroup, err := a.FetchOffsets(ctx, group)
	if err != nil {
		return nil, err
	}
	committed := byGroup[group]
	if len(committed) == 0 {
		return []PartitionLag{}, nil
	}

	ends, err := a.listLatestOffsets(ctx, committed)
	if err != nil {
		return nil, err
	}

	out := make([]PartitionLag, 0, len(committed))
	for _, co := range committed {
		end := ends[partKey{co.Topic, co.Partition}]
		lag := int64(0)
		if co.Offset >= 0 && end > co.Offset {
			lag = end - co.Offset
		}
		out = append(out, PartitionLag{
			Topic: co.Topic, Partition: co.Partition,
			Committed: co.Offset, LogEndOffset: end, Lag: lag,
		})
	}
	return out, nil
}

// listLatestOffsets resolves the log-end (latest) offset for each partition,
// grouping the requests by partition leader and refreshing metadata + retrying
// while leaders are unavailable.
func (a *Admin) listLatestOffsets(ctx context.Context, parts []CommittedOffset) (map[partKey]int64, error) {
	ends := map[partKey]int64{}
	err := retryRetriable(ctx, coordinatorRetry(a.client.cfg.Retry), func() error {
		byLeader := map[int32][]protocol.ListOffsetsPartition{}
		topics := map[string]struct{}{}
		for _, co := range parts {
			if _, done := ends[partKey{co.Topic, co.Partition}]; done {
				continue
			}
			topics[co.Topic] = struct{}{}
			leader, lerr := a.client.cluster.LeaderBroker(co.Topic, co.Partition)
			if lerr != nil {
				return newKafkaError(int16(ErrCodeNotLeaderForPart), co.Topic, co.Partition, "list offsets: leader unavailable")
			}
			byLeader[leader.NodeID] = append(byLeader[leader.NodeID], protocol.ListOffsetsPartition{
				Topic: co.Topic, Partition: co.Partition, Timestamp: -1, // -1 = latest
			})
		}
		for node, lparts := range byLeader {
			body := protocol.EncodeListOffsetsRequest(lparts, 0)
			rb, rerr := a.client.cluster.Request(ctx, node, protocol.APIListOffsets, protocol.VerListOffsets, body)
			if rerr != nil {
				return rerr
			}
			offs, derr := protocol.DecodeListOffsetsResponse(rb)
			if derr != nil {
				return derr
			}
			for _, o := range offs {
				if o.ErrorCode != 0 {
					ke := newKafkaError(o.ErrorCode, o.Topic, o.Partition, "list offsets failed")
					if IsRetriable(ke) {
						refreshTopics := make([]string, 0, len(topics))
						for t := range topics {
							refreshTopics = append(refreshTopics, t)
						}
						_ = a.client.cluster.Refresh(ctx, refreshTopics)
					}
					return ke
				}
				ends[partKey{o.Topic, o.Partition}] = o.Offset
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ends, nil
}
