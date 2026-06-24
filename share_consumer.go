package gokafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/internal/wire"
	"github.com/sinamohsenifar/gokafka/observe"
)

// ShareConsumer reads from topics using a KIP-932 share group (queue semantics).
type ShareConsumer struct {
	mu               sync.Mutex
	client           *Client
	topics           []string
	group            string
	memberID         string
	memberEpoch      int32
	coordID          int32
	hasCoord         bool
	assignments      []shareAssignment
	shareSession     map[int32]int32 // broker node -> session epoch
	hbCancel         context.CancelFunc
}

type shareAssignment struct {
	topic     string
	partition int32
	topicID   wire.UUID
	leader    int32
}

// ShareConsumer returns a KIP-932 share group consumer (Kafka 4.0+).
func (c *Client) ShareConsumer(topics []string) *ShareConsumer {
	return &ShareConsumer{
		client:       c,
		topics:       append([]string(nil), topics...),
		shareSession: map[int32]int32{},
	}
}

// Poll fetches the next batch of share-acquired records.
func (s *ShareConsumer) Poll(ctx context.Context) ([]Record, error) {
	if err := s.client.requireOpen(); err != nil {
		return nil, err
	}
	if s.client.cfg.ShareGroup == "" {
		return nil, ErrNoShareGroup
	}
	s.mu.Lock()
	s.group = s.client.cfg.ShareGroup
	needJoin := len(s.assignments) == 0
	s.mu.Unlock()

	if needJoin {
		if err := s.joinShareGroup(ctx); err != nil {
			s.client.observe.Metrics.OnConsume(0, err)
			return nil, err
		}
	}

	maxPoll := s.client.cfg.Consumer.MaxPollRecords
	if maxPoll <= 0 {
		maxPoll = 500
	}

	s.mu.Lock()
	assignments := append([]shareAssignment(nil), s.assignments...)
	group := s.group
	memberID := s.memberID
	s.mu.Unlock()

	byBroker := map[int32][]shareAssignment{}
	for _, a := range assignments {
		byBroker[a.leader] = append(byBroker[a.leader], a)
	}

	var out []Record
	for broker, parts := range byBroker {
		recs, err := s.fetchShare(ctx, broker, group, memberID, parts, maxPoll-len(out))
		if err != nil {
			if code, ok := protocol.APIErrorCode(err); ok {
				switch code {
				case 5, 6: // NOT_LEADER / LEADER_NOT_AVAILABLE
					_ = s.client.cluster.Refresh(ctx, s.topics)
					continue
				case 122, 123: // SHARE_SESSION_NOT_FOUND / INVALID_SHARE_SESSION_EPOCH
					s.resetShareSession(broker)
					recs, err = s.fetchShare(ctx, broker, group, memberID, parts, maxPoll-len(out))
				}
			}
			if err != nil {
				return nil, err
			}
		}
		out = append(out, recs...)
		if len(out) >= maxPoll {
			return out[:maxPoll], nil
		}
	}
	return out, nil
}

func (s *ShareConsumer) joinShareGroup(ctx context.Context) error {
	if err := s.client.cluster.Refresh(ctx, s.topics); err != nil {
		return err
	}
	coord, err := s.coordinator(ctx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if s.memberID == "" {
		s.memberID = newMemberUUID()
	}
	memberID := s.memberID
	s.mu.Unlock()

	var gotAssignment bool
	for attempt := 0; attempt < 30; attempt++ {
		s.mu.Lock()
		epoch := s.memberEpoch
		s.mu.Unlock()

		req := protocol.ShareGroupHeartbeatRequest{
			GroupID:              s.group,
			MemberID:             memberID,
			MemberEpoch:          epoch,
			SubscribedTopicNames: append([]string(nil), s.topics...),
		}
		if epoch > 0 {
			req.SubscribedTopicNames = nil
		}

		resp, err := s.sendShareHeartbeat(ctx, coord, req)
		if err != nil {
			if s.shouldRejoinShare(err) {
				s.mu.Lock()
				s.memberEpoch = 0
				s.mu.Unlock()
				s.invalidateCoordinator()
				coord, err = s.coordinator(ctx)
				if err != nil {
					return err
				}
				continue
			}
			if protocol.CoordinatorRetriable(protocolErrorCode(err)) {
				s.invalidateCoordinator()
				coord, err = s.coordinator(ctx)
				if err != nil {
					return err
				}
				continue
			}
			return err
		}

		s.mu.Lock()
		s.memberID = memberID
		if resp.MemberID != "" {
			s.memberID = resp.MemberID
		}
		s.memberEpoch = resp.MemberEpoch
		s.coordID = coord
		s.hasCoord = true
		s.mu.Unlock()

		if resp.HeartbeatIntervalMs > 0 {
			s.client.cfg.Consumer.HeartbeatInterval = time.Duration(resp.HeartbeatIntervalMs) * time.Millisecond
		}

		if len(resp.Assignment) > 0 {
			if err := s.applyShareAssignment(resp.Assignment); err != nil {
				return err
			}
			gotAssignment = true
			break
		}
		if resp.MemberEpoch <= 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !gotAssignment {
		return fmt.Errorf("gokafka: share group join: no partition assignment")
	}
	s.ensureShareHeartbeat()
	return s.shareHeartbeat(ctx)
}

func (s *ShareConsumer) applyShareAssignment(assign []protocol.TopicIDPartitions) error {
	var newAssign []shareAssignment
	for _, tp := range assign {
		name, ok := s.client.cluster.TopicNameByID(tp.TopicID)
		if !ok {
			return fmt.Errorf("gokafka: unknown topic id in share assignment")
		}
		for _, p := range tp.Partitions {
			leader, err := s.client.cluster.LeaderBroker(name, p)
			if err != nil {
				return err
			}
			newAssign = append(newAssign, shareAssignment{
				topic: name, partition: p, topicID: tp.TopicID, leader: leader.NodeID,
			})
		}
	}
	s.mu.Lock()
	s.assignments = newAssign
	for _, a := range newAssign {
		s.shareSession[a.leader] = 0
	}
	s.mu.Unlock()
	return nil
}

func (s *ShareConsumer) fetchShare(ctx context.Context, broker int32, group, memberID string, parts []shareAssignment, maxRecords int) ([]Record, error) {
	s.mu.Lock()
	epoch := s.shareSession[broker]
	s.mu.Unlock()

	fetchParts := make([]protocol.ShareFetchPartition, len(parts))
	for i, p := range parts {
		fetchParts[i] = protocol.ShareFetchPartition{
			TopicID: p.topicID, Partition: p.partition,
		}
	}
	ver := s.client.cluster.NegotiatedVersion(protocol.APIShareFetch, protocol.VerShareFetch)
	body := protocol.EncodeShareFetchRequest(ver, protocol.ShareFetchRequest{
		GroupID: group, MemberID: memberID, ShareSessionEpoch: epoch,
		MaxWaitMs: 500, MinBytes: 1, MaxBytes: 50 << 20, MaxRecords: int32(maxRecords), BatchSize: 1,
		Partitions: fetchParts,
	})
	rb, err := s.client.cluster.Request(ctx, broker, protocol.APIShareFetch, ver, body)
	if err != nil {
		return nil, err
	}
	resp, err := protocol.DecodeShareFetchResponse(rb, s.client.cluster.TopicNameByID)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	s.shareSession[broker] = epoch + 1
	s.mu.Unlock()

	var out []Record
	for _, fr := range resp.Records {
		out = append(out, Record{
			Topic: fr.Topic, Partition: fr.Partition, Offset: fr.Offset,
			Key: fr.Key, Value: fr.Value, Headers: fetchHeaders(fr.Headers),
			Timestamp: time.UnixMilli(fr.Timestamp),
		})
		s.client.observe.Metrics.OnConsume(len(fr.Value), nil)
	}
	return out, nil
}

// Acknowledge accepts delivery of processed records (ShareAckAccept).
func (s *ShareConsumer) Acknowledge(ctx context.Context, records ...Record) error {
	if len(records) == 0 {
		return nil
	}
	s.mu.Lock()
	group := s.group
	memberID := s.memberID
	s.mu.Unlock()

	byBroker := map[int32][]protocol.ShareFetchPartition{}
	for _, r := range records {
		id, ok := s.client.cluster.TopicIDByName(r.Topic)
		if !ok {
			return fmt.Errorf("gokafka: unknown topic %q", r.Topic)
		}
		leader, err := s.client.cluster.LeaderBroker(r.Topic, r.Partition)
		if err != nil {
			return err
		}
		byBroker[leader.NodeID] = append(byBroker[leader.NodeID], protocol.ShareFetchPartition{
			TopicID: id, Partition: r.Partition,
			AckBatches: []protocol.ShareAckBatch{{
				FirstOffset: r.Offset, LastOffset: r.Offset, Type: protocol.ShareAckAccept,
			}},
		})
	}

	for broker, parts := range byBroker {
		s.mu.Lock()
		epoch := s.shareSession[broker]
		s.mu.Unlock()
		body := protocol.EncodeShareAcknowledgeRequest(protocol.ShareAcknowledgeRequest{
			GroupID: group, MemberID: memberID, ShareSessionEpoch: epoch, Partitions: parts,
		})
		rb, err := s.client.cluster.Request(ctx, broker, protocol.APIShareAcknowledge, protocol.VerShareAcknowledge, body)
		if err != nil {
			return err
		}
		if _, err := protocol.DecodeShareAcknowledgeResponse(rb); err != nil {
			return err
		}
		s.mu.Lock()
		s.shareSession[broker] = epoch + 1
		s.mu.Unlock()
	}
	return nil
}

func (s *ShareConsumer) sendShareHeartbeat(ctx context.Context, coord int32, req protocol.ShareGroupHeartbeatRequest) (protocol.ShareGroupHeartbeatResponse, error) {
	body := protocol.EncodeShareGroupHeartbeatRequest(req)
	rb, err := s.client.cluster.Request(ctx, coord, protocol.APIShareGroupHeartbeat, protocol.VerShareGroupHeartbeat, body)
	if err != nil {
		return protocol.ShareGroupHeartbeatResponse{}, err
	}
	return protocol.DecodeShareGroupHeartbeatResponse(rb)
}

func (s *ShareConsumer) shareHeartbeat(ctx context.Context) error {
	s.mu.Lock()
	memberID := s.memberID
	epoch := s.memberEpoch
	group := s.group
	s.mu.Unlock()
	if memberID == "" {
		return nil
	}
	coord, err := s.coordinator(ctx)
	if err != nil {
		return err
	}
	req := protocol.ShareGroupHeartbeatRequest{
		GroupID: group, MemberID: memberID, MemberEpoch: epoch,
	}
	_, err = s.sendShareHeartbeat(ctx, coord, req)
	if err != nil && s.shouldRejoinShare(err) {
		return s.rejoinShare(ctx)
	}
	return err
}

func (s *ShareConsumer) rejoinShare(ctx context.Context) error {
	s.mu.Lock()
	s.assignments = nil
	s.memberEpoch = 0
	s.shareSession = map[int32]int32{}
	s.hasCoord = false
	s.mu.Unlock()
	return s.joinShareGroup(ctx)
}

func (s *ShareConsumer) shouldRejoinShare(err error) bool {
	switch protocolErrorCode(err) {
	case 25, 110, 112:
		return true
	default:
		return false
	}
}

func (s *ShareConsumer) coordinator(ctx context.Context) (int32, error) {
	s.mu.Lock()
	if s.hasCoord {
		id := s.coordID
		s.mu.Unlock()
		return id, nil
	}
	group := s.group
	s.mu.Unlock()
	id, err := s.client.cluster.FindCoordinator(ctx, group, protocol.CoordinatorGroup)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	s.coordID = id
	s.hasCoord = true
	s.mu.Unlock()
	return id, nil
}

func (s *ShareConsumer) invalidateCoordinator() {
	s.mu.Lock()
	s.hasCoord = false
	s.mu.Unlock()
}

func (s *ShareConsumer) resetShareSession(broker int32) {
	s.mu.Lock()
	s.shareSession[broker] = 0
	s.mu.Unlock()
}

func (s *ShareConsumer) ensureShareHeartbeat() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hbCancel != nil {
		return
	}
	hbCtx, cancel := context.WithCancel(context.Background())
	s.hbCancel = cancel
	go s.shareHeartbeatLoop(hbCtx)
}

func (s *ShareConsumer) stopShareHeartbeat() {
	s.mu.Lock()
	cancel := s.hbCancel
	s.hbCancel = nil
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *ShareConsumer) shareHeartbeatLoop(ctx context.Context) {
	interval := s.client.cfg.Consumer.HeartbeatInterval
	if interval <= 0 {
		interval = 3 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.shareHeartbeat(ctx); err != nil {
				s.client.observe.Log(ctx, observe.LevelWarn, "share heartbeat failed", observe.Error(err))
				if s.shouldRejoinShare(err) {
					_ = s.rejoinShare(ctx)
				}
			}
		}
	}
}

// Leave sends ShareGroupHeartbeat with MemberEpoch -1.
func (s *ShareConsumer) Leave(ctx context.Context) error {
	s.stopShareHeartbeat()
	s.mu.Lock()
	memberID := s.memberID
	group := s.group
	s.mu.Unlock()
	if memberID == "" {
		return nil
	}
	coord, err := s.coordinator(ctx)
	if err != nil {
		return err
	}
	req := protocol.ShareGroupHeartbeatRequest{
		GroupID: group, MemberID: memberID, MemberEpoch: -1,
	}
	_, err = s.sendShareHeartbeat(ctx, coord, req)
	return err
}

// Run polls and invokes handler; acknowledges after successful processing.
func (s *ShareConsumer) Run(ctx context.Context, h Handler) error {
	defer s.stopShareHeartbeat()
	for {
		select {
		case <-ctx.Done():
			return s.Leave(context.Background())
		default:
		}
		recs, err := s.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return s.Leave(context.Background())
			}
			return err
		}
		if len(recs) == 0 {
			continue
		}
		var processed []Record
		for _, r := range recs {
			if err := h(ctx, r); err != nil {
				return err
			}
			processed = append(processed, r)
		}
		if err := s.Acknowledge(ctx, processed...); err != nil {
			return err
		}
	}
}
