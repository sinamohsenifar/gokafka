package gokafka

import (
	"context"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// DeleteRecordsResult is the per-partition outcome of DeleteRecords.
type DeleteRecordsResult struct {
	Topic        string
	Partition    int32
	LowWatermark int64 // new earliest offset after deletion
	Err          error // non-nil if this partition failed
}

// DeleteRecords deletes all records before the given offset on each partition.
// Use -1 as the offset to delete up to the partition high watermark. Requests
// are routed to each partition's leader. The returned slice reports per-partition
// results; the error is non-nil only for transport/metadata failures.
func (a *Admin) DeleteRecords(ctx context.Context, offsets map[string]map[int32]int64) ([]DeleteRecordsResult, error) {
	if len(offsets) == 0 {
		return nil, nil
	}
	topics := make([]string, 0, len(offsets))
	for t := range offsets {
		topics = append(topics, t)
	}
	if err := a.client.cluster.RefreshIfStale(ctx, topics, false); err != nil {
		return nil, err
	}

	// Group (topic, partition, offset) by leader node.
	type tp struct {
		topic string
		part  int32
		off   int64
	}
	byLeader := map[int32][]tp{}
	for topic, parts := range offsets {
		for p, off := range parts {
			leader, ok := a.client.cluster.LeaderNodeID(topic, p)
			if !ok {
				return nil, newKafkaError(6, topic, p, "no leader for partition (delete records)")
			}
			byLeader[leader] = append(byLeader[leader], tp{topic, p, off})
		}
	}

	ver := a.client.cluster.NegotiatedVersion(protocol.APIDeleteRecords, protocol.VerDeleteRecords)
	if ver < 0 {
		ver = protocol.VerDeleteRecords
	}

	var out []DeleteRecordsResult
	for leader, items := range byLeader {
		req := map[string]map[int32]int64{}
		for _, it := range items {
			if req[it.topic] == nil {
				req[it.topic] = map[int32]int64{}
			}
			req[it.topic][it.part] = it.off
		}
		body := protocol.EncodeDeleteRecordsRequest(ver, req, 30000)
		resp, err := a.client.cluster.Request(ctx, leader, protocol.APIDeleteRecords, ver, body)
		if err != nil {
			return nil, err
		}
		results, err := protocol.DecodeDeleteRecordsResponse(ver, resp)
		if err != nil {
			return nil, err
		}
		for _, r := range results {
			res := DeleteRecordsResult{Topic: r.Topic, Partition: r.Partition, LowWatermark: r.LowWatermark}
			if r.ErrorCode != 0 {
				res.Err = newKafkaError(r.ErrorCode, r.Topic, r.Partition, "delete records failed")
			}
			out = append(out, res)
		}
	}
	return out, nil
}

// ElectionType selects the leader election strategy for ElectLeaders.
type ElectionType int8

const (
	// ElectionPreferred elects the preferred (first) replica as leader.
	ElectionPreferred ElectionType = ElectionType(protocol.ElectionPreferred)
	// ElectionUnclean allows electing an out-of-sync replica (possible data loss).
	ElectionUnclean ElectionType = ElectionType(protocol.ElectionUnclean)
)

// ElectLeadersResult is the per-partition outcome of ElectLeaders.
type ElectLeadersResult struct {
	Topic     string
	Partition int32
	Err       error // non-nil if election failed or was unnecessary
}

// ElectLeaders triggers a leader election. Pass nil topicPartitions to elect for
// every partition in the cluster. The returned slice reports per-partition
// results; the error is non-nil for transport or top-level failures.
func (a *Admin) ElectLeaders(ctx context.Context, electionType ElectionType, topicPartitions map[string][]int32) ([]ElectLeadersResult, error) {
	ver := a.client.cluster.NegotiatedVersion(protocol.APIElectLeaders, protocol.VerElectLeaders)
	if ver < 0 {
		ver = protocol.VerElectLeaders
	}
	body := protocol.EncodeElectLeadersRequest(ver, int8(electionType), topicPartitions, 30000)
	resp, err := a.requestAny(ctx, protocol.APIElectLeaders, ver, body)
	if err != nil {
		return nil, err
	}
	topErr, results, err := protocol.DecodeElectLeadersResponse(ver, resp)
	if err != nil {
		return nil, err
	}
	if topErr != 0 {
		return nil, newKafkaError(topErr, "", 0, "elect leaders failed")
	}
	out := make([]ElectLeadersResult, 0, len(results))
	for _, r := range results {
		res := ElectLeadersResult{Topic: r.Topic, Partition: r.Partition}
		if r.ErrorCode != 0 {
			res.Err = newKafkaError(r.ErrorCode, r.Topic, r.Partition, r.ErrorMessage)
		}
		out = append(out, res)
	}
	return out, nil
}
