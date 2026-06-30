package kfake

import "github.com/sinamohsenifar/gokafka/internal/wire"

// handleListOffsets decodes ListOffsets v3 (non-flexible) and returns the
// earliest (timestamp -2 -> 0) or latest (timestamp -1 / other -> log end)
// offset per requested partition.
func (b *Broker) handleListOffsets(_ int, body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // replica_id
		return nil, err
	}
	if _, err := buf.ReadInt8(); err != nil { // isolation_level (v2+)
		return nil, err
	}
	nTopics, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	type pres struct {
		part int32
		off  int64
	}
	type tres struct {
		name  string
		parts []pres
	}
	results := make([]tres, 0, nTopics)
	for i := int32(0); i < nTopics; i++ {
		name, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		tr := tres{name: name}
		for j := int32(0); j < nParts; j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			ts, err := buf.ReadInt64()
			if err != nil {
				return nil, err
			}
			off := int64(0)
			b.store.mu.Lock()
			if t := b.store.topics[name]; t != nil && int(part) < len(t.partitions) {
				if ts == -2 { // earliest
					off = 0
				} else { // latest (-1) or by-timestamp (approximated to latest)
					off = t.partitions[part].leo
				}
			}
			b.store.mu.Unlock()
			tr.parts = append(tr.parts, pres{part, off})
		}
		results = append(results, tr)
	}

	out := wire.NewBuffer(64)
	out.WriteInt32(0) // throttle_time_ms
	out.WriteInt32(int32(len(results)))
	for _, tr := range results {
		out.WriteString(tr.name)
		out.WriteInt32(int32(len(tr.parts)))
		for _, pr := range tr.parts {
			out.WriteInt32(pr.part)
			out.WriteInt16(0)  // error_code
			out.WriteInt64(-1) // timestamp
			out.WriteInt64(pr.off)
		}
	}
	return out.Bytes(), nil
}
