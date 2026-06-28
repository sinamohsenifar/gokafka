package gokafka

import (
	"context"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

func (a *Admin) decodeTopicResults(resp []byte, decode func([]byte) ([]protocol.TopicMutationResult, error), msg string) error {
	results, err := decode(resp)
	if err != nil {
		return err
	}
	if r, ok := protocol.FirstTopicError(results); ok {
		return newKafkaError(r.ErrorCode, r.Topic, 0, msg)
	}
	return nil
}

// TopicConfigAlteration changes a topic configuration entry.
type TopicConfigAlteration struct {
	Name  string
	Value *string // nil removes the config (revert to default)
}

// AlterTopicConfigs applies configuration changes to topics.
func (a *Admin) AlterTopicConfigs(ctx context.Context, alters map[string][]TopicConfigAlteration) error {
	if len(alters) == 0 {
		return nil
	}
	resources := make(map[string][]protocol.ConfigAlteration, len(alters))
	for topic, entries := range alters {
		out := make([]protocol.ConfigAlteration, len(entries))
		for i, e := range entries {
			out[i] = protocol.ConfigAlteration{Name: e.Name, Value: e.Value}
		}
		resources[topic] = out
	}
	ver := a.client.cluster.NegotiatedVersion(protocol.APIAlterConfigs, protocol.VerAlterConfigs)
	if ver < 0 {
		ver = protocol.VerAlterConfigs
	}
	body := protocol.EncodeAlterConfigsRequest(ver, resources)
	resp, err := a.requestAny(ctx, protocol.APIAlterConfigs, ver, body)
	if err != nil {
		return err
	}
	code, err := protocol.DecodeAlterConfigsResponse(ver, resp)
	if err != nil {
		return err
	}
	if code != 0 {
		return newKafkaError(code, "", 0, "alter configs failed")
	}
	return nil
}

// IncrementalAlterTopicConfigs applies incremental configuration changes (preferred on Kafka 2.3+).
func (a *Admin) IncrementalAlterTopicConfigs(ctx context.Context, alters map[string][]TopicConfigAlteration) error {
	if len(alters) == 0 {
		return nil
	}
	resources := make(map[string][]protocol.ConfigAlteration, len(alters))
	for topic, entries := range alters {
		out := make([]protocol.ConfigAlteration, len(entries))
		for i, e := range entries {
			out[i] = protocol.ConfigAlteration{Name: e.Name, Value: e.Value}
		}
		resources[topic] = out
	}
	ver := a.client.cluster.NegotiatedVersion(protocol.APIIncrementalAlterConfigs, protocol.VerIncrementalAlterConfigs)
	if ver < 0 {
		ver = protocol.VerIncrementalAlterConfigs
	}
	body := protocol.EncodeIncrementalAlterConfigsRequest(ver, resources)
	resp, err := a.requestAny(ctx, protocol.APIIncrementalAlterConfigs, ver, body)
	if err != nil {
		return err
	}
	code, err := protocol.DecodeIncrementalAlterConfigsResponse(ver, resp)
	if err != nil {
		return err
	}
	if code != 0 {
		return newKafkaError(code, "", 0, "incremental alter configs failed")
	}
	return nil
}

// CreatePartitions adds partitions to a topic.
func (a *Admin) CreatePartitions(ctx context.Context, topic string, count int32) error {
	ver := a.client.cluster.NegotiatedVersion(protocol.APICreatePartitions, protocol.VerCreatePartitions)
	if ver <= 0 {
		ver = protocol.VerCreatePartitions
	}
	body := protocol.EncodeCreatePartitionsRequest(ver, []protocol.CreatePartitionsSpec{{
		Topic: topic, Count: count,
	}}, 30000)
	resp, err := a.requestAny(ctx, protocol.APICreatePartitions, ver, body)
	if err != nil {
		return err
	}
	return a.decodeTopicResults(resp, func(b []byte) ([]protocol.TopicMutationResult, error) {
		return protocol.DecodeCreatePartitionsResponse(ver, b)
	}, "create partitions failed")
}

// DeleteConsumerGroupOffsets removes committed offsets for a consumer group.
func (a *Admin) DeleteConsumerGroupOffsets(ctx context.Context, groupID string, offsets map[string][]int32) error {
	if groupID == "" {
		return ErrNoConsumerGroup
	}
	ver := a.client.cluster.NegotiatedVersion(protocol.APIOffsetDelete, protocol.VerOffsetDelete)
	if ver < 0 {
		ver = protocol.VerOffsetDelete
	}
	body := protocol.EncodeOffsetDeleteRequest(ver, groupID, offsets)
	resp, err := a.requestAny(ctx, protocol.APIOffsetDelete, ver, body)
	if err != nil {
		return err
	}
	code, err := protocol.DecodeOffsetDeleteResponse(ver, resp)
	if err != nil {
		return err
	}
	if code != 0 {
		return newKafkaError(code, "", 0, "delete consumer group offsets failed")
	}
	return nil
}

// DeleteConsumerGroups deletes consumer groups via the group coordinator.
func (a *Admin) DeleteConsumerGroups(ctx context.Context, groups ...string) error {
	if len(groups) == 0 {
		return nil
	}
	for _, group := range groups {
		if group == "" {
			return ErrNoConsumerGroup
		}
		coord, err := a.client.cluster.FindCoordinator(ctx, group, protocol.CoordinatorGroup)
		if err != nil {
			return err
		}
		body := protocol.EncodeDeleteGroupsRequest([]string{group})
		resp, err := a.client.cluster.Request(ctx, coord, protocol.APIDeleteGroups, protocol.VerDeleteGroups, body)
		if err != nil {
			return err
		}
		results, err := protocol.DecodeDeleteGroupsResponse(resp)
		if err != nil {
			return err
		}
		if r, ok := protocol.FirstGroupError(results); ok {
			return newKafkaError(r.ErrorCode, r.GroupID, 0, "delete consumer group failed")
		}
	}
	return nil
}
