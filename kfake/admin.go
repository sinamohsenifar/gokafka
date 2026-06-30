package kfake

import "github.com/sinamohsenifar/gokafka/internal/wire"

const errTopicAlreadyExists = int16(36)

// handleCreateTopics decodes CreateTopics v4 (non-flexible) and creates the
// requested topics.
func (b *Broker) handleCreateTopics(_ int, body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	n, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	type result struct {
		name string
		code int16
	}
	results := make([]result, 0, n)
	for i := int32(0); i < n; i++ {
		name, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		parts, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		if _, err := buf.ReadInt16(); err != nil { // replication_factor
			return nil, err
		}
		na, err := buf.ReadInt32() // assignments
		if err != nil {
			return nil, err
		}
		for a := int32(0); a < na; a++ {
			if _, err := buf.ReadInt32(); err != nil { // partition_index
				return nil, err
			}
			nb, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			for j := int32(0); j < nb; j++ {
				if _, err := buf.ReadInt32(); err != nil {
					return nil, err
				}
			}
		}
		nc, err := buf.ReadInt32() // configs
		if err != nil {
			return nil, err
		}
		for c := int32(0); c < nc; c++ {
			if _, err := buf.ReadString(); err != nil {
				return nil, err
			}
			if _, err := buf.ReadString(); err != nil {
				return nil, err
			}
		}
		code := int16(0)
		b.store.mu.Lock()
		if !b.store.createTopic(name, parts) {
			code = errTopicAlreadyExists
		}
		b.store.mu.Unlock()
		results = append(results, result{name, code})
	}

	out := wire.NewBuffer(64)
	out.WriteInt32(0) // throttle_time_ms
	out.WriteInt32(int32(len(results)))
	for _, r := range results {
		out.WriteString(r.name)
		out.WriteInt16(r.code)
		out.WriteNullableString(nil) // error_message
	}
	return out.Bytes(), nil
}

// handleDeleteTopics decodes DeleteTopics v6 (flexible) and removes topics. The
// response matches the client's decoder: {name, error_code, tag} per topic.
func (b *Broker) handleDeleteTopics(_ int, body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, n)
	for i := 1; i < int(n); i++ {
		name, err := buf.ReadCompactNullableString()
		if err != nil {
			return nil, err
		}
		if _, err := buf.ReadUUID(); err != nil { // topic_id
			return nil, err
		}
		if err := skipTags(buf); err != nil {
			return nil, err
		}
		names = append(names, name)
	}

	out := wire.NewBuffer(64)
	out.WriteInt32(0) // throttle_time_ms
	out.WriteCompactArrayLen(len(names))
	for _, name := range names {
		b.store.mu.Lock()
		b.store.deleteTopic(name)
		b.store.mu.Unlock()
		out.WriteCompactString(name)
		out.WriteInt16(0) // error_code
		out.WriteEmptyTagSection()
	}
	out.WriteEmptyTagSection() // response tag
	return out.Bytes(), nil
}
