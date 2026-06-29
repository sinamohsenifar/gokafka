package gokafka

import "sync/atomic"

// Partitioner selects a partition for a record key.
type Partitioner interface {
	Partition(key []byte, numPartitions int) int32
}

// HashPartitioner uses FNV-1a (Kafka default style).
type HashPartitioner struct{}

func (HashPartitioner) Partition(key []byte, n int) int32 {
	if n <= 0 {
		return 0
	}
	if len(key) == 0 {
		return 0
	}
	h := fnv1a32(key)
	return int32(int(h) % n)
}

func fnv1a32(key []byte) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for _, b := range key {
		h ^= uint32(b)
		h *= prime32
	}
	return h
}

// RoundRobinPartitioner ignores keys and cycles partitions.
// Safe for concurrent use by multiple producer goroutines.
type RoundRobinPartitioner struct {
	counter uint32
}

func (r *RoundRobinPartitioner) Partition(_ []byte, n int) int32 {
	if n <= 0 {
		return 0
	}
	v := atomic.AddUint32(&r.counter, 1)
	return int32(v % uint32(n))
}
