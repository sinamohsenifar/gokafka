package gokafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
	"github.com/sinamohsenifar/gokafka/observe"
)

// Consumer reads from topics using a consumer group.
type Consumer struct {
	mu          sync.Mutex
	client      *Client
	topics      []string
	topicRegex  string // KIP-848 server-side RE2J subscription (next-gen only)
	group       string
	memberID    string
	generation  int32
	coordID     int32
	hasCoord    bool
	assignments []partitionOffset
	listener    RebalanceListener
	paused      map[partKey]struct{}
	hbCancel    context.CancelFunc
}

type partitionOffset struct {
	topic     string
	partition int32
	offset    int64
}

// Poll fetches the next batch of records from partition leaders.
func (c *Consumer) Poll(ctx context.Context) ([]Record, error) {
	if err := c.client.requireOpen(); err != nil {
		return nil, err
	}
	if c.client.cfg.ConsumerGroup == "" {
		return nil, ErrNoConsumerGroup
	}
	c.mu.Lock()
	c.group = c.client.cfg.ConsumerGroup
	needJoin := len(c.assignments) == 0
	c.mu.Unlock()

	if needJoin {
		if err := c.joinAndAssign(ctx); err != nil {
			c.client.observe.Metrics.OnConsume(0, err)
			c.client.observe.Log(ctx, observe.LevelError, "consumer join failed", observe.Error(err))
			return nil, err
		}
	}

	maxPoll := c.client.cfg.Consumer.MaxPollRecords
	if maxPoll <= 0 {
		maxPoll = 500
	}

	byNode := map[int32][]protocol.FetchPartition{}
	c.mu.Lock()
	assignments := append([]partitionOffset(nil), c.assignments...)
	c.mu.Unlock()
	for _, a := range assignments {
		if c.isPaused(a.topic, a.partition) {
			continue
		}
		b, err := c.client.cluster.LeaderBroker(a.topic, a.partition)
		if err != nil {
			return nil, err
		}
		fp := protocol.FetchPartition{
			Topic: a.topic, Partition: a.partition, Offset: a.offset, MaxBytes: 1 << 20,
			LeaderEpoch: c.client.cluster.LeaderEpoch(a.topic, a.partition),
		}
		// Fetch v13+ (KIP-516) identifies topics by UUID; resolve it from cluster
		// metadata (refreshed on assignment). If unknown, refresh and retry.
		if tid, ok := c.client.cluster.TopicIDByName(a.topic); ok {
			fp.TopicID = tid
		}
		byNode[b.NodeID] = append(byNode[b.NodeID], fp)
	}

	isolation := int8(0)
	if c.client.cfg.Consumer.IsolationLevel == IsolationReadCommitted {
		isolation = 1
	}

	group := c.group
	nodes := make([]int32, 0, len(byNode))
	for node := range byNode {
		nodes = append(nodes, node)
	}

	type nodeFetch struct {
		items []fetchItem
		err   error
	}
	fetches := make([]nodeFetch, len(nodes))
	if len(nodes) == 1 {
		fetches[0].items, fetches[0].err = c.fetchFromBroker(ctx, group, nodes[0], byNode[nodes[0]], isolation, maxPoll)
	} else {
		var wg sync.WaitGroup
		for i, node := range nodes {
			wg.Add(1)
			go func(i int, node int32) {
				defer wg.Done()
				fetches[i].items, fetches[i].err = c.fetchFromBroker(ctx, group, node, byNode[node], isolation, maxPoll)
			}(i, node)
		}
		wg.Wait()
	}
	for _, f := range fetches {
		if f.err != nil {
			if errors.Is(f.err, protocol.ErrRebalanceInProgress) {
				return c.handleFetchRebalance(ctx)
			}
			if errors.Is(f.err, protocol.ErrLeaderEpochChanged) || errors.Is(f.err, protocol.ErrUnknownTopicID) {
				_ = c.client.cluster.Refresh(ctx, c.topics)
				return nil, nil
			}
			return nil, f.err
		}
	}

	nodeItems := make([][]fetchItem, len(fetches))
	for i := range fetches {
		nodeItems[i] = fetches[i].items
	}
	out, cursor := aggregateFetches(nodeItems, maxPoll)
	for i := range out {
		c.client.observe.Metrics.OnConsume(len(out[i].Value), nil)
	}
	c.advanceDelivered(assignments, cursor)
	return out, nil
}

// aggregateFetches walks the per-node fetch items in order, delivering data
// records up to maxPoll and computing the per-partition cursor advance (the
// offset just past the last item actually delivered). A partition's cursor is
// set ONLY for items that fall before the max-poll cut, so records fetched
// beyond it — including a faster broker's tail that the old out[:maxPoll]
// truncation dropped after already bumping their offsets — are left out of the
// cursor map and re-fetched next Poll rather than silently skipped. Markers
// (rec == nil) advance the cursor without counting toward maxPoll, so a
// read_committed consumer never stalls re-fetching a marker, yet a marker past
// the cut is not applied over dropped data. Because the resulting cursor is the
// DELIVERED position (not the decode-ahead fetch position), a no-arg Commit can
// no longer commit past records Poll never returned.
func aggregateFetches(nodeItems [][]fetchItem, maxPoll int) ([]Record, map[partKey]int64) {
	out := make([]Record, 0, maxPoll)
	cursor := map[partKey]int64{}
	full := false
	for _, items := range nodeItems {
		if full {
			break
		}
		for _, it := range items {
			cursor[partKey{it.topic, it.partition}] = it.offset + 1
			if it.rec != nil {
				out = append(out, *it.rec)
				if maxPoll > 0 && len(out) >= maxPoll {
					full = true
					break
				}
			}
		}
	}
	return out, cursor
}

// advanceDelivered moves each partition's cursor to the offset just past the
// last record Poll delivered for it. It is a compare-and-set against the offset
// the fetch was issued from (fetchedFrom): if a concurrent Seek, a rebalance
// re-seed, or applyCommittedOffset repositioned the partition since the fetch,
// those records are stale and the advance is skipped (never moving the cursor
// backward or over a seek).
func (c *Consumer) advanceDelivered(fetchedFrom []partitionOffset, cursor map[partKey]int64) {
	if len(cursor) == 0 {
		return
	}
	from := make(map[partKey]int64, len(fetchedFrom))
	for _, a := range fetchedFrom {
		from[partKey{a.topic, a.partition}] = a.offset
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.assignments {
		pk := partKey{c.assignments[i].topic, c.assignments[i].partition}
		next, ok := cursor[pk]
		if !ok {
			continue
		}
		if f, ok := from[pk]; ok && c.assignments[i].offset == f && next > c.assignments[i].offset {
			c.assignments[i].offset = next
		}
	}
}

func (c *Consumer) handleFetchRebalance(ctx context.Context) ([]Record, error) {
	if c.isCooperative() {
		if err := c.cooperativeRejoin(ctx); err != nil {
			return nil, err
		}
	} else if err := c.Rebalance(ctx); err != nil {
		return nil, err
	}
	return c.Poll(ctx)
}

// fetchItem is one decoded fetch entry in wire (offset) order: a data record
// (rec != nil) or a transaction control / aborted-batch marker (rec == nil,
// advance-only). Poll advances a partition's cursor to the offset+1 of the last
// item it actually delivers, so a record dropped by the max-poll cut is
// re-fetched next Poll rather than silently skipped.
type fetchItem struct {
	topic     string
	partition int32
	offset    int64
	rec       *Record // nil for control / aborted-transaction markers
}

func (c *Consumer) fetchFromBroker(
	ctx context.Context,
	group string,
	node int32,
	parts []protocol.FetchPartition,
	isolation int8,
	maxRecords int,
) ([]fetchItem, error) {
	ver := c.client.cluster.NegotiatedVersion(protocol.APIFetch, protocol.VerFetch)
	body := protocol.EncodeFetchRequest(ver, group, parts, 500, 1, 50<<20, isolation)
	rb, err := c.client.cluster.Request(ctx, node, protocol.APIFetch, ver, body)
	if err != nil {
		c.client.observe.Metrics.OnConsume(0, err)
		return nil, err
	}
	fetched, err := protocol.DecodeFetchResponse(ver, rb, c.client.cluster.TopicNameByID)
	if err != nil {
		return nil, err
	}
	// Do NOT advance the cursor here — the cursor moves only for records Poll
	// actually delivers (see Poll's aggregation). Return items in wire order,
	// capped at maxRecords DATA records; markers are advance-only and don't count
	// toward the cap.
	items := make([]fetchItem, 0, len(fetched))
	data := 0
	for _, fr := range fetched {
		it := fetchItem{topic: fr.Topic, partition: fr.Partition, offset: fr.Offset}
		if !fr.Control {
			rec := Record{
				Topic: fr.Topic, Partition: fr.Partition, Offset: fr.Offset,
				Key: fr.Key, Value: fr.Value, Headers: fetchHeaders(fr.Headers),
				Timestamp: time.UnixMilli(fr.Timestamp),
			}
			it.rec = &rec
			data++
		}
		items = append(items, it)
		if maxRecords > 0 && data >= maxRecords {
			break
		}
	}
	return items, nil
}

// GroupMetadata returns the consumer's group generation and member identity for transactional offset commit.
func (c *Consumer) GroupMetadata() (generation int32, memberID, groupInstanceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.generation, c.memberID, c.client.cfg.Consumer.GroupInstanceID
}

func (c *Consumer) bumpOffset(topic string, part int32, off int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range c.assignments {
		if c.assignments[i].topic == topic && c.assignments[i].partition == part {
			c.assignments[i].offset = off
			return
		}
	}
}

// Commit commits consumed offsets to the consumer group coordinator. With
// explicit records it commits each record's offset+1 — commit only what you
// processed. With no records it commits the last offset Poll RETURNED for each
// partition (the delivered position), never the decode-ahead fetch position, so
// it cannot commit past records Poll never handed back. Like Kafka's
// commitSync(), the no-arg form assumes every record the last Poll returned was
// processed; pass the processed records explicitly if that is not guaranteed.
func (c *Consumer) Commit(ctx context.Context, records ...Record) error {
	return c.commitOffsets(ctx, records, 0)
}

// sleepCtx waits for d or until ctx is cancelled, returning ctx.Err() if cancelled.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *Consumer) commitOffsets(ctx context.Context, records []Record, attempt int) error {
	const maxCommitAttempts = 20
	if err := c.client.requireOpen(); err != nil {
		return err
	}
	offsets := map[string]map[int32]int64{}
	if len(records) == 0 {
		c.mu.Lock()
		for _, a := range c.assignments {
			if offsets[a.topic] == nil {
				offsets[a.topic] = map[int32]int64{}
			}
			offsets[a.topic][a.partition] = a.offset
		}
		c.mu.Unlock()
	} else {
		for _, r := range records {
			if offsets[r.Topic] == nil {
				offsets[r.Topic] = map[int32]int64{}
			}
			offsets[r.Topic][r.Partition] = r.Offset + 1
		}
	}

	coord, err := c.coordinator(ctx)
	if err != nil {
		return err
	}
	c.mu.Lock()
	group := c.group
	memberID := c.memberID
	generation := c.generation
	instanceID := c.client.cfg.Consumer.GroupInstanceID
	c.mu.Unlock()

	ver := c.client.cluster.NegotiatedVersion(protocol.APIOffsetCommit, protocol.VerOffsetCommit)
	body := protocol.EncodeOffsetCommitRequest(ver, group, memberID, instanceID, generation, offsets)
	rb, err := c.client.cluster.Request(ctx, coord, protocol.APIOffsetCommit, ver, body)
	if err != nil {
		return err
	}
	code, err := protocol.DecodeOffsetCommitResponse(ver, rb)
	if err != nil {
		return err
	}
	if code != 0 {
		if code == int16(ErrCodeRebalanceInProg) && attempt+1 < maxCommitAttempts {
			if err := sleepCtx(ctx, 500*time.Millisecond); err != nil {
				return err
			}
			return c.commitOffsets(ctx, records, attempt+1)
		}
		if c.shouldRejoin(newKafkaError(code, "", 0, "offset commit failed")) {
			if attempt+1 < maxCommitAttempts {
				if err := sleepCtx(ctx, 200*time.Millisecond); err != nil {
					return err
				}
				if err := c.rejoin(ctx); err != nil {
					return err
				}
				return c.commitOffsets(ctx, records, attempt+1)
			}
		}
		if protocol.CoordinatorRetriable(code) {
			c.invalidateCoordinator()
		}
		return newKafkaError(code, "", 0, "offset commit failed")
	}
	return nil
}

func (c *Consumer) joinAndAssign(ctx context.Context) error {
	if c.useNextGenGroup() {
		return c.joinAndAssign848(ctx)
	}
	if c.topicRegex != "" {
		return fmt.Errorf("gokafka: regex subscription (ConsumerPattern) requires GroupProtocolNextGen")
	}
	if err := c.client.cluster.Refresh(ctx, c.topics); err != nil {
		return err
	}
	coord, err := c.coordinator(ctx)
	if err != nil {
		return err
	}

	assignor := c.client.cfg.Consumer.Assignor.protocolName()
	sessionMs := int32(45000)
	if c.client.cfg.Consumer.SessionTimeout > 0 {
		sessionMs = int32(c.client.cfg.Consumer.SessionTimeout / time.Millisecond)
	}
	rebalanceMs := sessionMs
	if c.client.cfg.Consumer.RebalanceTimeout > 0 {
		rebalanceMs = int32(c.client.cfg.Consumer.RebalanceTimeout / time.Millisecond)
	}

	joinVer := c.client.cluster.NegotiatedVersion(protocol.APIJoinGroup, protocol.VerJoinGroup)
	syncVer := c.client.cluster.NegotiatedVersion(protocol.APISyncGroup, protocol.VerSyncGroup)

	var joined protocol.JoinGroupResponse
	var assignmentBytes []byte
joinLoop:
	for attempt := 0; attempt < 20; attempt++ {
	joinInner:
		for {
			joinBody := protocol.EncodeJoinGroupRequest(
				joinVer,
				c.group, c.memberID, assignor, c.client.cfg.Consumer.GroupInstanceID,
				c.topics, sessionMs, rebalanceMs, c.isCooperative(),
			)
			rb, err := c.client.cluster.Request(ctx, coord, protocol.APIJoinGroup, joinVer, joinBody)
			if err != nil {
				return err
			}
			joined, err = protocol.DecodeJoinGroupResponse(joinVer, rb)
			if errors.Is(err, protocol.ErrMemberIDRequired) {
				c.mu.Lock()
				c.memberID = joined.MemberID
				c.mu.Unlock()
				continue
			}
			if code, ok := protocol.APIErrorCode(err); ok && protocol.CoordinatorRetriable(code) {
				c.client.cluster.Invalidate(coord)
				c.invalidateCoordinator()
				coord, err = c.coordinator(ctx)
				if err != nil {
					return err
				}
				continue joinLoop
			}
			if err != nil {
				return err
			}
			break joinInner
		}

		c.mu.Lock()
		c.memberID = joined.MemberID
		c.generation = joined.GenerationID
		c.mu.Unlock()

		syncAssignments := map[string][]byte{joined.MemberID: {}}
		if joined.MemberID == joined.LeaderID {
			var members []protocol.MemberSubscription
			for mid, meta := range joined.Assignments {
				topics, err := protocol.DecodeConsumerSubscription(joinVer, meta)
				if err != nil {
					return err
				}
				members = append(members, protocol.MemberSubscription{MemberID: mid, Topics: topics})
			}
			if len(members) > 0 {
				// The leader assigns partitions for every topic ANY member
				// subscribes to, so its metadata must cover the union of all
				// members' subscriptions — not just this member's own topics.
				// Otherwise topics only other members subscribe to resolve to
				// zero partitions and are silently dropped from the assignment.
				topicSet := map[string]struct{}{}
				for _, m := range members {
					for _, t := range m.Topics {
						topicSet[t] = struct{}{}
					}
				}
				allTopics := make([]string, 0, len(topicSet))
				for t := range topicSet {
					allTopics = append(allTopics, t)
				}
				if err := c.client.cluster.Refresh(ctx, allTopics); err != nil {
					return err
				}
				meta := c.client.cluster.Metadata()
				topicParts := map[string][]int32{}
				for _, t := range meta.Topics {
					parts := make([]int32, 0, len(t.Partitions))
					for _, p := range t.Partitions {
						parts = append(parts, p.Partition)
					}
					topicParts[t.Name] = parts
				}
				syncAssignments = protocol.ComputeGroupAssignments(joined.Protocol, members, topicParts)
			}
		}

		syncBody := protocol.EncodeSyncGroupRequest(syncVer, c.group, joined.MemberID, joined.Protocol, c.client.cfg.Consumer.GroupInstanceID, joined.GenerationID, syncAssignments)
		rb, err := c.client.cluster.Request(ctx, coord, protocol.APISyncGroup, syncVer, syncBody)
		if err != nil {
			return err
		}
		assignmentBytes, err = protocol.DecodeSyncGroupResponse(syncVer, rb)
		if err != nil {
			if c.shouldRejoin(err) {
				c.invalidateCoordinator()
				coord, err = c.coordinator(ctx)
				if err != nil {
					return err
				}
				continue joinLoop
			}
			return err
		}
		break joinLoop
	}

	listenersNotified, err := c.applyAssignment(ctx, assignmentBytes, coord)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.coordID = coord
	c.hasCoord = true
	c.mu.Unlock()
	if err := c.loadCommittedOffsets(ctx, coord); err != nil {
		return err
	}
	if !listenersNotified {
		c.mu.Lock()
		c.notifyAssignedLocked(ctx)
		c.mu.Unlock()
	}
	c.ensureHeartbeat()
	_ = c.heartbeat(ctx)
	return nil
}

func (c *Consumer) applyAssignment(ctx context.Context, raw []byte, _ int32) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	parsed, err := protocol.ParseMemberAssignment(raw)
	if err != nil {
		return false, err
	}
	if c.isCooperative() && len(parsed) > 0 {
		c.applyAssignmentIncrementalLocked(ctx, parsed)
		return true, nil
	}
	c.notifyRevokedLocked(ctx)
	if len(parsed) > 0 {
		c.assignments = nil
		for _, a := range parsed {
			for _, p := range a.Partitions {
				c.assignments = append(c.assignments, partitionOffset{
					topic: a.Topic, partition: p, offset: 0,
				})
			}
		}
		return false, nil
	}
	if c.isCooperative() {
		c.assignments = nil
		return false, nil
	}
	// Fallback for single-member dev clusters when coordinator returns empty assignment.
	c.assignments = nil
	meta := c.client.cluster.Metadata()
	for _, topic := range c.topics {
		for _, t := range meta.Topics {
			if t.Name != topic {
				continue
			}
			for _, p := range t.Partitions {
				c.assignments = append(c.assignments, partitionOffset{
					topic: topic, partition: p.Partition, offset: 0,
				})
			}
		}
	}
	return false, nil
}

func (c *Consumer) applyAssignmentIncrementalLocked(ctx context.Context, parsed []protocol.TopicPartitionAssignment) {
	newSet := make(map[partKey]struct{})
	for _, a := range parsed {
		for _, p := range a.Partitions {
			newSet[partKey{a.Topic, p}] = struct{}{}
		}
	}
	kept := c.assignments[:0]
	var revoked []TopicPartition
	for _, a := range c.assignments {
		k := partKey{a.topic, a.partition}
		if _, ok := newSet[k]; ok {
			kept = append(kept, a)
		} else {
			revoked = append(revoked, TopicPartition{Topic: a.topic, Partition: a.partition, Offset: a.offset})
		}
	}
	if c.listener != nil && len(revoked) > 0 {
		c.listener.OnPartitionsRevoked(ctx, revoked)
	}
	existing := make(map[partKey]struct{}, len(kept))
	for _, a := range kept {
		existing[partKey{a.topic, a.partition}] = struct{}{}
	}
	var assigned []TopicPartition
	for _, a := range parsed {
		for _, p := range a.Partitions {
			k := partKey{a.Topic, p}
			if _, ok := existing[k]; ok {
				continue
			}
			kept = append(kept, partitionOffset{topic: a.Topic, partition: p, offset: 0})
			assigned = append(assigned, TopicPartition{Topic: a.Topic, Partition: p})
		}
	}
	c.assignments = kept
	if c.listener != nil && len(assigned) > 0 {
		c.listener.OnPartitionsAssigned(ctx, assigned)
	}
}

func (c *Consumer) loadCommittedOffsets(ctx context.Context, coord int32) error {
	c.mu.Lock()
	assignments := append([]partitionOffset(nil), c.assignments...)
	group := c.group
	memberID := c.memberID
	c.mu.Unlock()
	if len(assignments) == 0 {
		return nil
	}
	parts := make([]protocol.OffsetFetchPartition, len(assignments))
	for i, a := range assignments {
		parts[i] = protocol.OffsetFetchPartition{Topic: a.topic, Partition: a.partition}
	}
	// require_stable=true (KIP-447): block the OffsetFetch until any pending
	// transactional offset commits resolve, so a resuming consumer never reads a
	// stale committed offset in an exactly-once pipeline (matches franz-go).
	body := protocol.EncodeOffsetFetchRequest(protocol.VerOffsetFetchSingle, group, memberID, parts, true)

	// Drive completeness off the ASSIGNMENT set, not the OffsetFetch response.
	// applyAssignment seeds every assigned partition at offset 0; a partition
	// that is omitted from the response (a top-level COORDINATOR_LOAD returns an
	// empty topics array) or comes back with a transient per-partition code
	// (UNSTABLE_OFFSET_COMMIT while a transactional commit is in flight,
	// coordinator load) would otherwise be left at 0 — silently re-reading the
	// whole partition from the log start (mass duplication) or tripping
	// OFFSET_OUT_OF_RANGE once retention advances the log-start offset. Retry the
	// fetch until every assigned partition is resolved (committed offset applied,
	// or its reset ladder run when there is no commit); never fall back to 0.
	need := make(map[partKey]struct{}, len(assignments))
	for _, a := range assignments {
		need[partKey{a.topic, a.partition}] = struct{}{}
	}
	const maxAttempts = 20
	backoff := 100 * time.Millisecond
	for attempt := 0; ; attempt++ {
		rb, err := c.client.cluster.Request(ctx, coord, protocol.APIOffsetFetch, protocol.VerOffsetFetchSingle, body)
		if err != nil {
			return err
		}
		committed, err := protocol.DecodeOffsetFetchResponse(protocol.VerOffsetFetchSingle, rb)
		if err != nil {
			return err
		}
		for _, co := range committed {
			k := partKey{co.Topic, co.Partition}
			if _, want := need[k]; !want {
				continue // already resolved, or not an assigned partition
			}
			if co.ErrorCode != 0 {
				if offsetFetchRetriable(co.ErrorCode) {
					continue // transient — leave in need and retry
				}
				return newKafkaError(co.ErrorCode, co.Topic, co.Partition, "offset fetch failed")
			}
			if err := c.applyCommittedOffset(ctx, co); err != nil {
				return err
			}
			delete(need, k)
		}
		if len(need) == 0 {
			return nil
		}
		if attempt >= maxAttempts-1 {
			return fmt.Errorf("gokafka: offset fetch left %d assigned partition(s) unresolved after %d attempts (coordinator still loading?)", len(need), maxAttempts)
		}
		if err := sleepCtx(ctx, backoff); err != nil {
			return err
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

// applyCommittedOffset positions one assigned partition from its OffsetFetch
// result: a committed offset seeks there; a partition with no commit (Offset<0)
// runs the configured auto-offset-reset ladder (by-duration / earliest / latest).
func (c *Consumer) applyCommittedOffset(ctx context.Context, co protocol.CommittedOffset) error {
	if co.Offset >= 0 {
		c.bumpOffset(co.Topic, co.Partition, co.Offset)
		return nil
	}
	if d := c.client.cfg.Consumer.OffsetResetDuration; d > 0 {
		return c.SeekToTime(ctx, co.Topic, time.Now().Add(-d), co.Partition)
	}
	if c.client.cfg.Consumer.ConsumeFromBeginning {
		if c.client.cfg.Consumer.IsolationLevel == IsolationReadCommitted {
			return c.Seek(co.Topic, co.Partition, 0)
		}
		return c.SeekToBeginning(ctx, co.Topic, co.Partition)
	}
	return c.SeekToEnd(ctx, co.Topic, co.Partition)
}

// offsetFetchRetriable reports whether a per-partition OffsetFetch error code is
// transient and the fetch should be retried (rather than surfaced or, worse,
// left at the offset-0 default). UNSTABLE_OFFSET_COMMIT is returned while a
// transactional offset commit is still in flight under require_stable.
func offsetFetchRetriable(code int16) bool {
	switch ErrorCode(code) {
	case ErrCodeCoordinatorLoad, ErrCodeCoordinatorNotAvailable, ErrCodeNotCoordinator,
		ErrCodeUnstableOffsetCommit:
		return true
	}
	return false
}

func (c *Consumer) ensureHeartbeat() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hbCancel != nil {
		return
	}
	hbCtx, cancel := context.WithCancel(context.Background())
	c.hbCancel = cancel
	go c.heartbeatLoop(hbCtx)
}

func (c *Consumer) stopHeartbeat() {
	c.mu.Lock()
	cancel := c.hbCancel
	c.hbCancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *Consumer) coordinator(ctx context.Context) (int32, error) {
	c.mu.Lock()
	if c.hasCoord {
		id := c.coordID
		c.mu.Unlock()
		return id, nil
	}
	group := c.group
	if group == "" {
		group = c.client.cfg.ConsumerGroup
	}
	c.mu.Unlock()

	id, err := c.client.cluster.FindCoordinator(ctx, group, protocol.CoordinatorGroup)
	if err != nil {
		return 0, err
	}
	c.mu.Lock()
	c.coordID = id
	c.hasCoord = true
	c.mu.Unlock()
	return id, nil
}

func (c *Consumer) invalidateCoordinator() {
	c.mu.Lock()
	c.hasCoord = false
	c.mu.Unlock()
}

func (c *Consumer) shouldRejoin(err error) bool {
	code, ok := protocol.APIErrorCode(err)
	if !ok {
		var ke *KafkaError
		if errors.As(err, &ke) {
			code = int16(ke.Code)
			ok = true
		}
	}
	if !ok {
		return false
	}
	switch code {
	case int16(ErrCodeRebalanceInProg), int16(ErrCodeNotCoordinator),
		25, 22: // UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION
		return true
	default:
		return protocol.CoordinatorRetriable(code)
	}
}

func (c *Consumer) rejoin(ctx context.Context) error {
	c.mu.Lock()
	c.assignments = nil
	c.hasCoord = false
	c.mu.Unlock()
	return c.joinAndAssign(ctx)
}

func (c *Consumer) isCooperative() bool {
	return c.client.cfg.Consumer.Assignor == AssignorCooperativeSticky
}

func (c *Consumer) cooperativeRejoin(ctx context.Context) error {
	c.notifyRevoked(ctx)
	return c.joinAndAssign(ctx)
}

func fetchHeaders(h [][2][]byte) []Header {
	if len(h) == 0 {
		return nil
	}
	out := make([]Header, len(h))
	for i, pair := range h {
		out[i] = Header{Key: string(pair[0]), Value: pair[1]}
	}
	return out
}
