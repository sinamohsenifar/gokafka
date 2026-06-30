package kfake

import (
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// advertised is the set of APIs the mock supports, with the max version it
// implements. The client negotiates down to these, so it speaks exactly the
// versions the handlers below decode/encode.
var advertised = []struct{ key, max int16 }{
	{apiProduce, 9}, {apiFetch, 12}, {apiListOffsets, 3}, {apiMetadata, 12},
	{apiOffsetCommit, 8}, {apiOffsetFetch, 8}, {apiFindCoordinator, 3},
	{apiJoinGroup, 6}, {apiHeartbeat, 4}, {apiLeaveGroup, 5}, {apiSyncGroup, 5},
	{apiApiVersions, 3}, {apiCreateTopics, 4}, {apiDeleteTopics, 6}, {apiInitProducerID, 4},
}

func (b *Broker) handleApiVersions() ([]byte, error) {
	buf := wire.NewBuffer(128)
	buf.WriteInt16(0) // error_code
	buf.WriteCompactArrayLen(len(advertised))
	for _, a := range advertised {
		buf.WriteInt16(a.key)
		buf.WriteInt16(0)     // min_version
		buf.WriteInt16(a.max) // max_version
		buf.WriteEmptyTagSection()
	}
	buf.WriteInt32(0)          // throttle_time_ms
	buf.WriteEmptyTagSection() // response body tag (no finalized features)
	return buf.Bytes(), nil
}

// handleMetadata returns the mock as the only broker plus all current topics
// (v12 flexible). The request body is ignored — all topics are returned and the
// client filters to what it needs.
func (b *Broker) handleMetadata(_ int, _ []byte) ([]byte, error) {
	b.store.mu.Lock()
	defer b.store.mu.Unlock()

	buf := wire.NewBuffer(256)
	buf.WriteInt32(0) // throttle_time_ms

	// brokers: just self.
	buf.WriteCompactArrayLen(1)
	buf.WriteInt32(b.nodeID)
	buf.WriteCompactString(b.host)
	buf.WriteInt32(b.port)
	buf.WriteCompactNullableString(nil) // rack
	buf.WriteEmptyTagSection()

	buf.WriteCompactNullableString(strPtr("kfake-cluster")) // cluster_id
	buf.WriteInt32(b.nodeID)                                // controller_id

	buf.WriteCompactArrayLen(len(b.store.topics))
	for _, t := range b.store.topics {
		buf.WriteInt16(0) // topic error_code
		buf.WriteCompactNullableString(strPtr(t.name))
		buf.WriteUUID(t.id)
		buf.WriteBool(false) // is_internal
		buf.WriteCompactArrayLen(len(t.partitions))
		for p := range t.partitions {
			buf.WriteInt16(0)        // partition error_code
			buf.WriteInt32(int32(p)) // partition_index
			buf.WriteInt32(b.nodeID) // leader_id
			buf.WriteInt32(0)        // leader_epoch
			buf.WriteCompactArrayLen(1)
			buf.WriteInt32(b.nodeID) // replicas
			buf.WriteCompactArrayLen(1)
			buf.WriteInt32(b.nodeID)    // isr
			buf.WriteCompactArrayLen(0) // offline
			buf.WriteEmptyTagSection()
		}
		buf.WriteInt32(0) // topic_authorized_operations (v8+)
		buf.WriteEmptyTagSection()
	}
	buf.WriteEmptyTagSection() // response tag
	return buf.Bytes(), nil
}

func strPtr(s string) *string { return &s }

// --- stubs filled in subsequent files ---
