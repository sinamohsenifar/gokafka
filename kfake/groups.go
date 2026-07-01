package kfake

import "github.com/sinamohsenifar/gokafka/internal/wire"

// handleFindCoordinator (v3 flex) always returns the mock itself as coordinator.
func (b *Broker) handleFindCoordinator(_ int, _ []byte) ([]byte, error) {
	out := wire.NewBuffer(64)
	out.WriteInt32(0)                   // throttle_time_ms
	out.WriteInt16(0)                   // error_code
	out.WriteCompactNullableString(nil) // error_message
	out.WriteInt32(b.nodeID)            // node_id
	out.WriteCompactString(b.host)
	out.WriteInt32(b.port)
	out.WriteEmptyTagSection()
	return out.Bytes(), nil
}

// handleJoinGroup (v6 flex) admits the single member, capturing its subscription
// metadata and echoing it back so the client (acting as leader) can assign.
func (b *Broker) handleJoinGroup(_ int, body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	group, err := buf.ReadCompactString()
	if err != nil {
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // session_timeout_ms
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // rebalance_timeout_ms
		return nil, err
	}
	memberID, err := buf.ReadCompactString()
	if err != nil {
		return nil, err
	}
	if _, err := buf.ReadCompactNullableString(); err != nil { // group_instance_id
		return nil, err
	}
	if _, err := buf.ReadCompactString(); err != nil { // protocol_type
		return nil, err
	}
	nProto, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	var assignor string
	var meta []byte
	for i := 1; i < int(nProto); i++ {
		name, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		m, err := buf.ReadCompactBytes()
		if err != nil {
			return nil, err
		}
		if err := skipTags(buf); err != nil {
			return nil, err
		}
		if i == 1 {
			assignor, meta = name, m
		}
	}

	b.store.mu.Lock()
	g := b.store.group(group)
	if memberID == "" {
		memberID = "kfake-member-1"
	}
	g.memberID = memberID
	g.metadata = meta
	g.assignor = assignor
	g.generation++
	gen := g.generation
	b.store.mu.Unlock()

	out := wire.NewBuffer(128)
	out.WriteInt32(0)                // throttle_time_ms
	out.WriteInt16(0)                // error_code
	out.WriteInt32(gen)              // generation_id
	out.WriteCompactString(assignor) // protocol_name
	out.WriteCompactString(memberID) // leader
	out.WriteCompactString(memberID) // member_id
	out.WriteCompactArrayLen(1)      // members
	out.WriteCompactString(memberID)
	out.WriteCompactNullableString(nil) // group_instance_id
	out.WriteCompactBytes(meta)
	out.WriteEmptyTagSection()
	out.WriteEmptyTagSection() // response tag
	return out.Bytes(), nil
}

// handleSyncGroup (v5 flex) echoes back the assignment the leader computed for
// this member.
func (b *Broker) handleSyncGroup(_ int, body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	group, err := buf.ReadCompactString()
	if err != nil {
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // generation_id
		return nil, err
	}
	memberID, err := buf.ReadCompactString()
	if err != nil {
		return nil, err
	}
	if _, err := buf.ReadCompactNullableString(); err != nil { // group_instance_id
		return nil, err
	}
	if _, err := buf.ReadCompactString(); err != nil { // protocol_type
		return nil, err
	}
	protoName, err := buf.ReadCompactString() // protocol_name
	if err != nil {
		return nil, err
	}
	nAssign, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	var assignment []byte
	for i := 1; i < int(nAssign); i++ {
		mid, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		a, err := buf.ReadCompactBytes()
		if err != nil {
			return nil, err
		}
		if err := skipTags(buf); err != nil {
			return nil, err
		}
		if mid == memberID {
			assignment = a
		}
	}
	_ = group

	out := wire.NewBuffer(64)
	out.WriteInt32(0)                  // throttle_time_ms
	out.WriteInt16(0)                  // error_code
	out.WriteCompactString("consumer") // protocol_type
	out.WriteCompactString(protoName)  // protocol_name
	out.WriteCompactBytes(assignment)  // assignment
	out.WriteEmptyTagSection()
	return out.Bytes(), nil
}

// handleHeartbeat (v4 flex) always succeeds for the single member.
func (b *Broker) handleHeartbeat(_ int, _ []byte) ([]byte, error) {
	out := wire.NewBuffer(16)
	out.WriteInt32(0) // throttle_time_ms
	out.WriteInt16(0) // error_code
	out.WriteEmptyTagSection()
	return out.Bytes(), nil
}

// handleLeaveGroup (v5 flex) clears the member.
func (b *Broker) handleLeaveGroup(_ int, body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	if group, err := buf.ReadCompactString(); err == nil {
		b.store.mu.Lock()
		if g, ok := b.store.groups[group]; ok {
			g.memberID = ""
		}
		b.store.mu.Unlock()
	}
	out := wire.NewBuffer(16)
	out.WriteInt32(0) // throttle_time_ms
	out.WriteInt16(0) // error_code
	out.WriteEmptyTagSection()
	return out.Bytes(), nil
}

// handleOffsetCommit (v8 flex) stores committed offsets for the group.
func (b *Broker) handleOffsetCommit(_ int, body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	group, err := buf.ReadCompactString()
	if err != nil {
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // generation_id
		return nil, err
	}
	if _, err := buf.ReadCompactString(); err != nil { // member_id
		return nil, err
	}
	if _, err := buf.ReadCompactNullableString(); err != nil { // group_instance_id
		return nil, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	type pc struct {
		name  string
		parts []int32
	}
	committed := []pc{}
	b.store.mu.Lock()
	g := b.store.group(group)
	for i := 1; i < int(nTopics); i++ {
		name, err := buf.ReadCompactString()
		if err != nil {
			b.store.mu.Unlock()
			return nil, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			b.store.mu.Unlock()
			return nil, err
		}
		entry := pc{name: name}
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				b.store.mu.Unlock()
				return nil, err
			}
			off, err := buf.ReadInt64()
			if err != nil {
				b.store.mu.Unlock()
				return nil, err
			}
			if _, err := buf.ReadInt32(); err != nil { // committed_leader_epoch
				b.store.mu.Unlock()
				return nil, err
			}
			if _, err := buf.ReadCompactNullableString(); err != nil { // metadata
				b.store.mu.Unlock()
				return nil, err
			}
			if err := skipTags(buf); err != nil {
				b.store.mu.Unlock()
				return nil, err
			}
			if g.offsets[name] == nil {
				g.offsets[name] = map[int32]int64{}
			}
			g.offsets[name][part] = off
			entry.parts = append(entry.parts, part)
		}
		if err := skipTags(buf); err != nil {
			b.store.mu.Unlock()
			return nil, err
		}
		committed = append(committed, entry)
	}
	b.store.mu.Unlock()

	out := wire.NewBuffer(64)
	out.WriteInt32(0) // throttle_time_ms
	out.WriteCompactArrayLen(len(committed))
	for _, t := range committed {
		out.WriteCompactString(t.name)
		out.WriteCompactArrayLen(len(t.parts))
		for _, p := range t.parts {
			out.WriteInt32(p)
			out.WriteInt16(0) // error_code
			out.WriteEmptyTagSection()
		}
		out.WriteEmptyTagSection()
	}
	out.WriteEmptyTagSection()
	return out.Bytes(), nil
}

// handleOffsetFetch returns committed offsets. v8+ is the batched multi-group
// form (KIP-709, used by Admin.FetchOffsets / ConsumerGroupLag); v7 is the
// single-group form the consumer uses.
func (b *Broker) handleOffsetFetch(ver int, body []byte) ([]byte, error) {
	if ver >= 8 {
		return b.handleOffsetFetchMultiGroup(body)
	}
	buf := wire.FromBytes(body)
	group, err := buf.ReadCompactString()
	if err != nil {
		return nil, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	type tq struct {
		name  string
		parts []int32
	}
	reqs := []tq{}
	for i := 1; i < int(nTopics); i++ {
		name, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		entry := tq{name: name}
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			entry.parts = append(entry.parts, part)
		}
		if err := skipTags(buf); err != nil {
			return nil, err
		}
		reqs = append(reqs, entry)
	}
	// require_stable + request tag are not needed by the mock.

	faultCode := b.takeOffsetFetchFault()

	b.store.mu.Lock()
	g := b.store.group(group)
	out := wire.NewBuffer(64)
	out.WriteInt32(0) // throttle_time_ms
	out.WriteCompactArrayLen(len(reqs))
	for _, t := range reqs {
		out.WriteCompactString(t.name)
		out.WriteCompactArrayLen(len(t.parts))
		for _, p := range t.parts {
			off := int64(-1)
			if m, ok := g.offsets[t.name]; ok {
				if v, ok := m[p]; ok {
					off = v
				}
			}
			if faultCode != 0 {
				off = -1 // no committed offset reported alongside the injected error
			}
			out.WriteInt32(p)
			out.WriteInt64(off)                 // committed_offset
			out.WriteInt32(-1)                  // committed_leader_epoch
			out.WriteCompactNullableString(nil) // metadata
			out.WriteInt16(faultCode)           // error_code (0 = ok)
			out.WriteEmptyTagSection()
		}
		out.WriteEmptyTagSection()
	}
	b.store.mu.Unlock()
	out.WriteInt16(0)          // group-level error_code
	out.WriteEmptyTagSection() // response tag
	return out.Bytes(), nil
}

// handleOffsetFetchMultiGroup handles OffsetFetch v8 (batched groups, KIP-709).
// Each requested group has null topics (all topics), so all of the group's
// committed offsets are returned.
func (b *Broker) handleOffsetFetchMultiGroup(body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	nGroups, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	groups := make([]string, 0, int(nGroups))
	for i := 1; i < int(nGroups); i++ {
		gid, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		nTopics, err := buf.ReadUvarint() // topics (0 = null = all)
		if err != nil {
			return nil, err
		}
		for j := 1; j < int(nTopics); j++ {
			if _, err := buf.ReadCompactString(); err != nil { // name
				return nil, err
			}
			nParts, err := buf.ReadUvarint()
			if err != nil {
				return nil, err
			}
			for k := 1; k < int(nParts); k++ {
				if _, err := buf.ReadInt32(); err != nil {
					return nil, err
				}
			}
			if err := skipTags(buf); err != nil {
				return nil, err
			}
		}
		if err := skipTags(buf); err != nil { // group tag
			return nil, err
		}
		groups = append(groups, gid)
	}

	out := wire.NewBuffer(128)
	out.WriteInt32(0) // throttle_time_ms
	out.WriteCompactArrayLen(len(groups))
	b.store.mu.Lock()
	for _, gid := range groups {
		out.WriteCompactString(gid)
		g := b.store.group(gid)
		out.WriteCompactArrayLen(len(g.offsets))
		for topic, parts := range g.offsets {
			out.WriteCompactString(topic)
			out.WriteCompactArrayLen(len(parts))
			for part, off := range parts {
				out.WriteInt32(part)
				out.WriteInt64(off)                 // committed_offset
				out.WriteInt32(-1)                  // committed_leader_epoch
				out.WriteCompactNullableString(nil) // metadata
				out.WriteInt16(0)                   // error_code
				out.WriteEmptyTagSection()
			}
			out.WriteEmptyTagSection() // topic tag
		}
		out.WriteInt16(0)          // group error_code
		out.WriteEmptyTagSection() // group tag
	}
	b.store.mu.Unlock()
	out.WriteEmptyTagSection() // response tag
	return out.Bytes(), nil
}
