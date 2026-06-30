package kfake

import (
	"sync"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// partitionLog is the in-memory log for one partition. Record batches are stored
// opaquely (already base-offset-patched) and served back verbatim on Fetch — the
// mock never parses individual records.
type partitionLog struct {
	batches [][]byte
	leo     int64 // log end offset (next offset to assign)
}

// topicState holds a topic's partitions and identity.
type topicState struct {
	name       string
	id         wire.UUID
	partitions []*partitionLog
}

// store is the broker's in-memory state: topics, partition logs, and committed
// group offsets. All access goes through the broker's mutex.
type store struct {
	mu       sync.Mutex
	topics   map[string]*topicState
	groups   map[string]*groupState // groupID -> committed offsets + membership
	nextUUID byte
}

// groupState tracks a consumer group's committed offsets and its single member.
// kfake models one consumer per group — the common unit-test shape.
type groupState struct {
	offsets    map[string]map[int32]int64 // topic -> partition -> committed offset
	memberID   string
	metadata   []byte // the member's subscription, echoed back in JoinGroup
	assignor   string // negotiated protocol name
	generation int32
}

func newStore() *store {
	return &store{topics: map[string]*topicState{}, groups: map[string]*groupState{}}
}

// createTopic adds a topic with n partitions, returning false if it already
// exists. Caller holds the lock.
func (s *store) createTopic(name string, n int32) bool {
	if _, ok := s.topics[name]; ok {
		return false
	}
	if n <= 0 {
		n = 1
	}
	s.nextUUID++
	var id wire.UUID
	id[15] = s.nextUUID // deterministic non-zero topic id
	ts := &topicState{name: name, id: id, partitions: make([]*partitionLog, n)}
	for i := range ts.partitions {
		ts.partitions[i] = &partitionLog{}
	}
	s.topics[name] = ts
	return true
}

func (s *store) deleteTopic(name string) bool {
	if _, ok := s.topics[name]; !ok {
		return false
	}
	delete(s.topics, name)
	return true
}

func (s *store) group(id string) *groupState {
	g, ok := s.groups[id]
	if !ok {
		g = &groupState{offsets: map[string]map[int32]int64{}}
		s.groups[id] = g
	}
	return g
}
