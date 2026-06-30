package gokafka

import (
	"context"
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// Admin exposes Kafka admin operations.
type Admin struct {
	client *Client
}

func (a *Admin) requestAny(ctx context.Context, apiKey, ver int16, body []byte) ([]byte, error) {
	if err := a.client.requireOpen(); err != nil {
		return nil, err
	}
	if !a.client.cluster.AdvertisesAPI(apiKey) {
		// The broker did not advertise this API (e.g. ElectLeaders or delegation
		// tokens on Redpanda). Return a clear error instead of letting the broker
		// reset the connection into an opaque EOF.
		return nil, fmt.Errorf("gokafka: broker does not support API key %d (%s)", apiKey, protocol.APIName(apiKey))
	}
	return a.client.cluster.RequestAny(ctx, apiKey, ver, body)
}

// CreateTopic creates a topic with partitions and replication factor.
func (a *Admin) CreateTopic(ctx context.Context, name string, partitions int32, replication int16) error {
	return a.CreateTopics(ctx, TopicSpec{
		Name: name, Partitions: partitions, ReplicationFactor: replication,
	})
}

// TopicSpec describes a topic to create.
type TopicSpec struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string // optional topic configs (retention, cleanup.policy, etc.)
}

// CreateTopics creates one or more topics with optional configuration.
func (a *Admin) CreateTopics(ctx context.Context, specs ...TopicSpec) error {
	if len(specs) == 0 {
		return nil
	}
	topics := make(map[string]protocol.TopicCreate, len(specs))
	names := make([]string, 0, len(specs))
	for _, s := range specs {
		topics[s.Name] = protocol.TopicCreate{
			Partitions: s.Partitions, ReplicationFactor: s.ReplicationFactor, Configs: s.Configs,
		}
		names = append(names, s.Name)
	}
	body := protocol.EncodeCreateTopicsRequest(topics)
	resp, err := a.requestAny(ctx, protocol.APICreateTopics, protocol.VerCreateTopics, body)
	if err != nil {
		return err
	}
	if err := a.decodeTopicResults(resp, protocol.DecodeCreateTopicsResponse, "create topic failed"); err != nil {
		return err
	}
	return a.client.cluster.Refresh(ctx, names)
}

// DeleteTopics deletes topics by name.
func (a *Admin) DeleteTopics(ctx context.Context, topics ...string) error {
	ver := a.client.cluster.NegotiatedVersion(protocol.APIDeleteTopics, protocol.VerDeleteTopics)
	if ver <= 0 {
		ver = protocol.VerDeleteTopics
	}
	body := protocol.EncodeDeleteTopicsRequest(ver, topics)
	resp, err := a.requestAny(ctx, protocol.APIDeleteTopics, ver, body)
	if err != nil {
		return err
	}
	return a.decodeTopicResults(resp, protocol.DecodeDeleteTopicsResponse, "delete topic failed")
}

// ListTopics returns topic names from cached metadata.
func (a *Admin) ListTopics(ctx context.Context) ([]string, error) {
	if err := a.client.requireOpen(); err != nil {
		return nil, err
	}
	if err := a.client.cluster.Refresh(ctx, nil); err != nil {
		return nil, err
	}
	meta := a.client.cluster.Metadata()
	out := make([]string, 0, len(meta.Topics))
	for _, t := range meta.Topics {
		out = append(out, t.Name)
	}
	return out, nil
}

// TopicPartitions returns partition count for a topic.
func (a *Admin) TopicPartitions(ctx context.Context, topic string) (int, error) {
	if err := a.client.cluster.Refresh(ctx, []string{topic}); err != nil {
		return 0, err
	}
	meta := a.client.cluster.Metadata()
	for _, t := range meta.Topics {
		if t.Name == topic {
			return len(t.Partitions), nil
		}
	}
	return 0, ErrTopicNotFound
}

// ConsumerGroupSummary lists a consumer group id and protocol type.
type ConsumerGroupSummary struct {
	GroupID      string
	ProtocolType string
}

// GroupMemberSummary describes a consumer group member.
type GroupMemberSummary struct {
	MemberID   string
	ClientID   string
	ClientHost string
}

// ShareGroupMemberSummary describes a share group member.
type ShareGroupMemberSummary struct {
	MemberID             string
	MemberEpoch          int32
	ClientID             string
	ClientHost           string
	SubscribedTopicNames []string
}

// ShareGroupDescription is detailed share group metadata from ShareGroupDescribe.
type ShareGroupDescription struct {
	GroupID         string
	State           string
	GroupEpoch      int32
	AssignmentEpoch int32
	AssignorName    string
	Members         []ShareGroupMemberSummary
	ErrorCode       ErrorCode
}

// ConsumerGroupDescription is detailed group metadata from DescribeGroups.
type ConsumerGroupDescription struct {
	GroupID      string
	State        string
	ProtocolType string
	Members      []GroupMemberSummary
	ErrorCode    ErrorCode
}

// ConfigEntry describes a broker or topic configuration property.
type ConfigEntry struct {
	Name       string
	Value      string
	IsDefault  bool
	IsReadOnly bool
}

// ListConsumerGroups returns consumer group ids visible to the cluster.
func (a *Admin) ListConsumerGroups(ctx context.Context) ([]ConsumerGroupSummary, error) {
	body := protocol.EncodeListGroupsRequest()
	rb, err := a.requestAny(ctx, protocol.APIListGroups, protocol.VerListGroups, body)
	if err != nil {
		return nil, err
	}
	groups, err := protocol.DecodeListGroupsResponse(rb)
	if err != nil {
		return nil, err
	}
	out := make([]ConsumerGroupSummary, len(groups))
	for i, g := range groups {
		out[i] = ConsumerGroupSummary{GroupID: g.GroupID, ProtocolType: g.ProtocolType}
	}
	return out, nil
}

// DescribeConsumerGroups returns state and members for the given group ids.
func (a *Admin) DescribeConsumerGroups(ctx context.Context, groups ...string) ([]ConsumerGroupDescription, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	body := protocol.EncodeDescribeGroupsRequest(groups)
	rb, err := a.requestAny(ctx, protocol.APIDescribeGroups, protocol.VerDescribeGroups, body)
	if err != nil {
		return nil, err
	}
	raw, err := protocol.DecodeDescribeGroupsResponse(rb)
	if err != nil {
		return nil, err
	}
	out := make([]ConsumerGroupDescription, len(raw))
	for i, g := range raw {
		desc := ConsumerGroupDescription{
			GroupID: g.GroupID, State: g.State, ProtocolType: g.ProtocolType,
			ErrorCode: ErrorCode(g.ErrorCode),
		}
		for _, m := range g.Members {
			desc.Members = append(desc.Members, GroupMemberSummary{
				MemberID: m.MemberID, ClientID: m.ClientID, ClientHost: m.ClientHost,
			})
		}
		out[i] = desc
	}
	return out, nil
}

// DescribeShareGroups returns state and members for KIP-932 share groups (Kafka 4.1+).
func (a *Admin) DescribeShareGroups(ctx context.Context, groups ...string) ([]ShareGroupDescription, error) {
	if len(groups) == 0 {
		return nil, nil
	}
	ver := a.client.cluster.NegotiatedVersion(protocol.APIShareGroupDescribe, protocol.VerShareGroupDescribe)
	if ver <= 0 {
		return nil, fmt.Errorf("gokafka: broker does not support ShareGroupDescribe")
	}
	body := protocol.EncodeShareGroupDescribeRequest(groups, false)
	rb, err := a.requestAny(ctx, protocol.APIShareGroupDescribe, ver, body)
	if err != nil {
		return nil, err
	}
	raw, err := protocol.DecodeShareGroupDescribeResponse(rb)
	if err != nil {
		return nil, err
	}
	out := make([]ShareGroupDescription, len(raw))
	for i, g := range raw {
		desc := ShareGroupDescription{
			GroupID: g.GroupID, State: g.GroupState, GroupEpoch: g.GroupEpoch,
			AssignmentEpoch: g.AssignmentEpoch, AssignorName: g.AssignorName,
			ErrorCode: ErrorCode(g.ErrorCode),
		}
		for _, m := range g.Members {
			desc.Members = append(desc.Members, ShareGroupMemberSummary{
				MemberID: m.MemberID, MemberEpoch: m.MemberEpoch,
				ClientID: m.ClientID, ClientHost: m.ClientHost,
				SubscribedTopicNames: append([]string(nil), m.SubscribedTopicNames...),
			})
		}
		out[i] = desc
	}
	return out, nil
}

func (a *Admin) describeConfigs(ctx context.Context, resources []protocol.ConfigResource) (map[string][]ConfigEntry, error) {
	if err := a.client.requireOpen(); err != nil {
		return nil, err
	}
	ver := a.client.cluster.NegotiatedVersion(protocol.APIDescribeConfigs, protocol.VerDescribeConfigs)
	if ver <= 0 {
		ver = 1
	}
	body := protocol.EncodeDescribeConfigsRequest(ver, resources)
	if err := a.client.cluster.Refresh(ctx, nil); err != nil {
		return nil, err
	}
	nodeID := a.client.cluster.Metadata().Controller
	for _, r := range resources {
		if r.Type == protocol.ConfigResourceBroker {
			var id int32
			if _, err := fmt.Sscanf(r.Name, "%d", &id); err == nil && id > 0 {
				nodeID = id
				break
			}
		}
	}
	rb, err := a.client.cluster.Request(ctx, nodeID, protocol.APIDescribeConfigs, ver, body)
	if err != nil {
		return nil, err
	}
	raw, err := protocol.DecodeDescribeConfigsResponse(ver, rb)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]ConfigEntry, len(raw))
	for name, entries := range raw {
		ce := make([]ConfigEntry, len(entries))
		for i, e := range entries {
			ce[i] = ConfigEntry{Name: e.Name, Value: e.Value, IsDefault: e.IsDefault, IsReadOnly: e.IsReadOnly}
		}
		out[name] = ce
	}
	return out, nil
}

func (a *Admin) DescribeBrokerConfigs(ctx context.Context, brokerIDs ...int32) (map[int32][]ConfigEntry, error) {
	resources := make([]protocol.ConfigResource, len(brokerIDs))
	for i, id := range brokerIDs {
		resources[i] = protocol.ConfigResource{
			Type: protocol.ConfigResourceBroker,
			Name: fmt.Sprintf("%d", id),
		}
	}
	raw, err := a.describeConfigs(ctx, resources)
	if err != nil {
		return nil, err
	}
	out := make(map[int32][]ConfigEntry, len(brokerIDs))
	for name, entries := range raw {
		var id int32
		fmt.Sscanf(name, "%d", &id)
		ce := make([]ConfigEntry, len(entries))
		for i, e := range entries {
			ce[i] = ConfigEntry{Name: e.Name, Value: e.Value, IsDefault: e.IsDefault, IsReadOnly: e.IsReadOnly}
		}
		out[id] = ce
	}
	return out, nil
}
func (a *Admin) DescribeTopicConfigs(ctx context.Context, topics ...string) (map[string][]ConfigEntry, error) {
	resources := make([]protocol.ConfigResource, len(topics))
	for i, t := range topics {
		resources[i] = protocol.ConfigResource{Type: protocol.ConfigResourceTopic, Name: t}
	}
	raw, err := a.describeConfigs(ctx, resources)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]ConfigEntry, len(raw))
	for name, entries := range raw {
		ce := make([]ConfigEntry, len(entries))
		for i, e := range entries {
			ce[i] = ConfigEntry{Name: e.Name, Value: e.Value, IsDefault: e.IsDefault, IsReadOnly: e.IsReadOnly}
		}
		out[name] = ce
	}
	return out, nil
}

// DescribeTopic returns partition metadata including leaders and ISR.
func (a *Admin) DescribeTopic(ctx context.Context, topic string) (TopicDescription, error) {
	if err := a.client.requireOpen(); err != nil {
		return TopicDescription{}, err
	}
	if err := a.client.cluster.Refresh(ctx, []string{topic}); err != nil {
		return TopicDescription{}, err
	}
	meta := a.client.cluster.Metadata()
	for _, t := range meta.Topics {
		if t.Name != topic {
			continue
		}
		desc := TopicDescription{Name: topic, ErrorCode: t.ErrorCode}
		for _, p := range t.Partitions {
			desc.Partitions = append(desc.Partitions, PartitionDescription{
				ID: p.Partition, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR, ErrorCode: p.ErrorCode,
			})
		}
		return desc, nil
	}
	return TopicDescription{}, ErrTopicNotFound
}

// TopicDescription is detailed topic metadata.
type TopicDescription struct {
	Name       string
	ErrorCode  int16
	Partitions []PartitionDescription
}

// PartitionDescription describes a single topic partition.
type PartitionDescription struct {
	ID        int32
	Leader    int32
	Replicas  []int32
	ISR       []int32
	ErrorCode int16
}
