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
	mu           sync.Mutex
	client       *Client
	topics       []string
	group        string
	memberID     string
	memberEpoch  int32
	coordID      int32
	hasCoord     bool
	assignments  []shareAssignment
	shareSession map[int32]int32 // broker node -> session epoch
	hbCancel     context.CancelFunc
	hbInterval   time.Duration // broker-negotiated heartbeat interval (guarded by mu)
	// pendingAccept holds the records returned by the last Poll that still await
	// implicit auto-acceptance (ShareAckImplicit only; guarded by mu).
	pendingAccept []Record
}

// ackMode reports the configured share acknowledgement mode.
func (s *ShareConsumer) ackMode() ShareAckMode { return s.client.cfg.Consumer.ShareAckMode }

// trackDelivered records the batch returned by Poll for later implicit
// auto-acceptance. No-op in explicit mode.
func (s *ShareConsumer) trackDelivered(recs []Record) {
	if s.ackMode() != ShareAckImplicit {
		return
	}
	s.mu.Lock()
	s.pendingAccept = append([]Record(nil), recs...)
	s.mu.Unlock()
}

// clearPending drops the given records from the implicit auto-accept set after
// the caller has explicitly terminal-acknowledged them (Accept/Release/Reject),
// so they are not auto-accepted again on the next Poll. Renew leaves them in.
func (s *ShareConsumer) clearPending(records []Record) {
	if s.ackMode() != ShareAckImplicit {
		return
	}
	type rk struct {
		topic     string
		partition int32
		offset    int64
	}
	done := make(map[rk]struct{}, len(records))
	for _, r := range records {
		done[rk{r.Topic, r.Partition, r.Offset}] = struct{}{}
	}
	s.mu.Lock()
	kept := make([]Record, 0, len(s.pendingAccept))
	for _, p := range s.pendingAccept {
		if _, removed := done[rk{p.Topic, p.Partition, p.Offset}]; removed {
			continue
		}
		kept = append(kept, p)
	}
	s.pendingAccept = kept
	s.mu.Unlock()
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

	// Implicit acknowledgement: accept the records delivered by the previous Poll
	// (minus any the caller explicitly handled) before acquiring the next batch.
	if s.ackMode() == ShareAckImplicit {
		s.mu.Lock()
		prev := s.pendingAccept
		s.pendingAccept = nil
		s.mu.Unlock()
		if len(prev) > 0 {
			if err := s.acknowledge(ctx, protocol.ShareAckAccept, prev); err != nil {
				return nil, err
			}
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

	// The first ShareFetch against a partition only initializes broker-side
	// share state and returns no records, so a single fetch round can come back
	// empty even when data is available. Mirror KafkaShareConsumer.poll: keep
	// running fetch rounds until records are acquired or the context ends.
	for {
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
				out = out[:maxPoll]
				s.trackDelivered(out)
				return out, nil
			}
		}
		if len(out) > 0 {
			s.trackDelivered(out)
			return out, nil
		}
		// No records yet: stop if the caller's context is done (returning empty,
		// like a poll timeout), otherwise wait briefly before the next fetch round.
		// The short backoff avoids hammering the share-partition leader (and its
		// connection) while the broker initializes share state / waits for data.
		select {
		case <-ctx.Done():
			return nil, nil
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// applyShareStartOffset honours WithConsumeFromBeginning for share groups by
// setting the group-level share.auto.offset.reset config to "earliest" before
// the share-partition start offset is initialized on the first fetch. Without
// this the broker default ("latest") is used and records produced before the
// consumer joins are never delivered. It must run before the first ShareFetch.
func (s *ShareConsumer) applyShareStartOffset(ctx context.Context) error {
	if !s.client.cfg.Consumer.ConsumeFromBeginning {
		return nil
	}
	val := "earliest"
	ver := s.client.cluster.NegotiatedVersion(protocol.APIIncrementalAlterConfigs, protocol.VerIncrementalAlterConfigs)
	if ver < 0 {
		ver = protocol.VerIncrementalAlterConfigs
	}
	body := protocol.EncodeIncrementalAlterConfigsRequest(ver, protocol.ConfigResourceGroup,
		map[string][]protocol.ConfigAlteration{
			s.group: {{Name: "share.auto.offset.reset", Value: &val}},
		})
	resp, err := s.client.cluster.RequestAny(ctx, protocol.APIIncrementalAlterConfigs, ver, body)
	if err != nil {
		return err
	}
	code, err := protocol.DecodeIncrementalAlterConfigsResponse(ver, resp)
	if err != nil {
		return err
	}
	if code != 0 {
		return newKafkaError(code, "", 0, "set share.auto.offset.reset failed")
	}
	return nil
}

func (s *ShareConsumer) joinShareGroup(ctx context.Context) error {
	if err := s.client.cluster.Refresh(ctx, s.topics); err != nil {
		return err
	}
	if err := s.applyShareStartOffset(ctx); err != nil {
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
	for attempt := 0; attempt < 60; attempt++ {
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
		if resp.HeartbeatIntervalMs > 0 {
			s.hbInterval = time.Duration(resp.HeartbeatIntervalMs) * time.Millisecond
		}
		s.mu.Unlock()

		if len(resp.Assignment) > 0 {
			if err := s.applyShareAssignment(resp.Assignment); err != nil {
				return err
			}
			gotAssignment = true
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
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
		if fr.Control { // skip transaction control markers
			continue
		}
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
	return s.acknowledge(ctx, protocol.ShareAckAccept, records)
}

// Release returns records to the share group so another consumer may deliver
// them again (ShareAckRelease).
func (s *ShareConsumer) Release(ctx context.Context, records ...Record) error {
	return s.acknowledge(ctx, protocol.ShareAckRelease, records)
}

// Reject permanently rejects records (e.g. unprocessable / poison messages) so
// they are not redelivered (ShareAckReject).
func (s *ShareConsumer) Reject(ctx context.Context, records ...Record) error {
	return s.acknowledge(ctx, protocol.ShareAckReject, records)
}

// Renew extends the acquisition lock on still-in-flight records (ShareAckRenew,
// KIP-1222) so long processing does not lose the lock. Requires a broker that
// supports ShareAcknowledge v2 (Kafka 4.3+); otherwise it returns an error.
func (s *ShareConsumer) Renew(ctx context.Context, records ...Record) error {
	if v := s.client.cluster.NegotiatedVersion(protocol.APIShareAcknowledge, protocol.VerShareAcknowledge); v < 2 {
		return fmt.Errorf("gokafka: share Renew requires ShareAcknowledge v2 (broker negotiated v%d)", v)
	}
	return s.acknowledge(ctx, protocol.ShareAckRenew, records)
}

func (s *ShareConsumer) acknowledge(ctx context.Context, ackType protocol.ShareAckType, records []Record) error {
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
				FirstOffset: r.Offset, LastOffset: r.Offset, Type: ackType,
			}},
		})
	}

	ver := s.client.cluster.NegotiatedVersion(protocol.APIShareAcknowledge, protocol.VerShareAcknowledge)
	for broker, parts := range byBroker {
		s.mu.Lock()
		epoch := s.shareSession[broker]
		s.mu.Unlock()
		body := protocol.EncodeShareAcknowledgeRequest(ver, protocol.ShareAcknowledgeRequest{
			GroupID: group, MemberID: memberID, ShareSessionEpoch: epoch, Partitions: parts,
		})
		rb, err := s.client.cluster.Request(ctx, broker, protocol.APIShareAcknowledge, ver, body)
		if err != nil {
			return err
		}
		if _, err := protocol.DecodeShareAcknowledgeResponse(ver, rb); err != nil {
			return err
		}
		s.mu.Lock()
		s.shareSession[broker] = epoch + 1
		s.mu.Unlock()
	}
	// A terminal acknowledgement removes these records from the implicit
	// auto-accept set; Renew keeps them (still in flight).
	if ackType != protocol.ShareAckRenew {
		s.clearPending(records)
	}
	return nil
}

func (s *ShareConsumer) sendShareHeartbeat(ctx context.Context, coord int32, req protocol.ShareGroupHeartbeatRequest) (protocol.ShareGroupHeartbeatResponse, error) {
	ver := s.client.cluster.NegotiatedVersion(protocol.APIShareGroupHeartbeat, protocol.VerShareGroupHeartbeat)
	body := protocol.EncodeShareGroupHeartbeatRequest(req)
	rb, err := s.client.cluster.Request(ctx, coord, protocol.APIShareGroupHeartbeat, ver, body)
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
	s.mu.Lock()
	interval := s.hbInterval
	s.mu.Unlock()
	if interval <= 0 {
		interval = s.client.cfg.Consumer.HeartbeatInterval
	}
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
	// Implicit ack: accept any still-pending delivered records before leaving
	// (best-effort — the session is about to end).
	if s.ackMode() == ShareAckImplicit {
		s.mu.Lock()
		prev := s.pendingAccept
		s.pendingAccept = nil
		s.mu.Unlock()
		if len(prev) > 0 {
			_ = s.acknowledge(ctx, protocol.ShareAckAccept, prev)
		}
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
