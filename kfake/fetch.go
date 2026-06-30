package kfake

import (
	"encoding/binary"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// handleFetch decodes Fetch v12 (flexible, name-based) and returns the record
// batches stored at or after each partition's fetch offset.
func (b *Broker) handleFetch(_ int, body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // replica_id
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // max_wait_ms
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // min_bytes
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // max_bytes
		return nil, err
	}
	if _, err := buf.ReadInt8(); err != nil { // isolation_level
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // session_id
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // session_epoch
		return nil, err
	}

	type preq struct {
		part   int32
		offset int64
	}
	type treq struct {
		name  string
		parts []preq
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	reqs := make([]treq, 0, int(nTopics))
	for i := 1; i < int(nTopics); i++ {
		name, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		tr := treq{name: name}
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			if _, err := buf.ReadInt32(); err != nil { // current_leader_epoch
				return nil, err
			}
			offset, err := buf.ReadInt64() // fetch_offset
			if err != nil {
				return nil, err
			}
			if _, err := buf.ReadInt32(); err != nil { // last_fetched_epoch
				return nil, err
			}
			if _, err := buf.ReadInt64(); err != nil { // log_start_offset
				return nil, err
			}
			if _, err := buf.ReadInt32(); err != nil { // partition_max_bytes
				return nil, err
			}
			if err := skipTags(buf); err != nil { // partition tag
				return nil, err
			}
			tr.parts = append(tr.parts, preq{part, offset})
		}
		if err := skipTags(buf); err != nil { // topic tag
			return nil, err
		}
		reqs = append(reqs, tr)
	}
	// forgotten_topics + rack_id + request tag are not needed by the mock.

	out := wire.NewBuffer(256)
	out.WriteInt32(0) // throttle_time_ms
	out.WriteInt16(0) // error_code
	out.WriteInt32(0) // session_id
	out.WriteCompactArrayLen(len(reqs))
	for _, tr := range reqs {
		out.WriteCompactString(tr.name)
		out.WriteCompactArrayLen(len(tr.parts))
		for _, pr := range tr.parts {
			records, hwm := b.collectBatches(tr.name, pr.part, pr.offset)
			out.WriteInt32(pr.part)
			out.WriteInt16(0)           // error_code
			out.WriteInt64(hwm)         // high_watermark
			out.WriteInt64(hwm)         // last_stable_offset
			out.WriteInt64(0)           // log_start_offset
			out.WriteCompactArrayLen(0) // aborted_transactions (empty)
			out.WriteInt32(-1)          // preferred_read_replica
			out.WriteCompactBytes(records)
			out.WriteEmptyTagSection()
		}
		out.WriteEmptyTagSection() // topic tag
	}
	out.WriteEmptyTagSection() // response tag
	return out.Bytes(), nil
}

// collectBatches returns the concatenated record batches at or after fetchOffset
// for a partition, plus the partition's high watermark (log end offset).
func (b *Broker) collectBatches(topic string, part int32, fetchOffset int64) ([]byte, int64) {
	b.store.mu.Lock()
	defer b.store.mu.Unlock()
	t := b.store.topics[topic]
	if t == nil || int(part) < 0 || int(part) >= len(t.partitions) {
		return nil, 0
	}
	pl := t.partitions[part]
	var out []byte
	for _, batch := range pl.batches {
		if len(batch) < 8 {
			continue
		}
		base := int64(binary.BigEndian.Uint64(batch[:8]))
		if base >= fetchOffset {
			out = append(out, batch...)
		}
	}
	return out, pl.leo
}
