package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// OffsetFetchPartition identifies a partition to fetch committed offsets for.
type OffsetFetchPartition struct {
	Topic     string
	Partition int32
}

// CommittedOffset is a group offset from OffsetFetch.
type CommittedOffset struct {
	Topic     string
	Partition int32
	Offset    int64
	Metadata  string
	ErrorCode int16
}

func EncodeOffsetFetchRequest(ver int16, group string, _ string, parts []OffsetFetchPartition, requireStable bool) []byte {
	if ver <= 0 {
		ver = VerOffsetFetchSingle
	}
	if ver >= 6 {
		return encodeOffsetFetchRequestFlex(ver, group, parts, requireStable)
	}
	return encodeOffsetFetchRequestLegacy(ver, group, parts, requireStable)
}

func encodeOffsetFetchRequestLegacy(ver int16, group string, parts []OffsetFetchPartition, requireStable bool) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteString(group)
	topics := map[string][]int32{}
	order := make([]string, 0)
	for _, p := range parts {
		if _, ok := topics[p.Topic]; !ok {
			order = append(order, p.Topic)
		}
		topics[p.Topic] = append(topics[p.Topic], p.Partition)
	}
	buf.WriteInt32(int32(len(order)))
	for _, topic := range order {
		buf.WriteString(topic)
		writeInt32Array(buf, topics[topic])
	}
	if ver >= 7 {
		buf.WriteBool(requireStable) // require_stable (KIP-447, v7+)
	}
	return buf.Bytes()
}

func encodeOffsetFetchRequestFlex(ver int16, group string, parts []OffsetFetchPartition, requireStable bool) []byte {
	// OffsetFetch flexible request: group_id, topics[name, partition_indexes[]],
	// require_stable (v7+), request tag. partition_indexes is a primitive int32
	// array (no per-element tag); each topic struct has a trailing tag.
	buf := wire.NewBuffer(64)
	buf.WriteCompactString(group)
	topics := map[string][]int32{}
	order := make([]string, 0)
	for _, p := range parts {
		if _, ok := topics[p.Topic]; !ok {
			order = append(order, p.Topic)
		}
		topics[p.Topic] = append(topics[p.Topic], p.Partition)
	}
	buf.WriteCompactArrayLen(len(order))
	for _, topic := range order {
		buf.WriteCompactString(topic)
		partsForTopic := topics[topic]
		buf.WriteCompactArrayLen(len(partsForTopic))
		for _, part := range partsForTopic {
			buf.WriteInt32(part)
		}
		buf.WriteEmptyTagSection() // topic struct tag
	}
	if ver >= 7 {
		buf.WriteBool(requireStable) // require_stable (KIP-447, v7+)
	}
	buf.WriteEmptyTagSection() // request tag
	return buf.Bytes()
}

func DecodeOffsetFetchResponse(ver int16, body []byte) ([]CommittedOffset, error) {
	if ver <= 0 {
		ver = VerOffsetFetchSingle
	}
	if ver >= 6 {
		return decodeOffsetFetchResponseFlex(body)
	}
	return decodeOffsetFetchResponseLegacy(ver, body)
}

func decodeOffsetFetchResponseLegacy(ver int16, body []byte) ([]CommittedOffset, error) {
	buf := wire.FromBytes(body)
	if ver >= 3 {
		if _, err := buf.ReadInt32(); err != nil {
			return nil, err
		}
	}
	nTopics, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	var out []CommittedOffset
	for i := int32(0); i < nTopics; i++ {
		topic, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		for j := int32(0); j < nParts; j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			off, err := buf.ReadInt64()
			if err != nil {
				return nil, err
			}
			if ver >= 5 {
				if _, err := buf.ReadInt32(); err != nil {
					return nil, err
				}
			}
			meta, err := readNullableString(buf)
			if err != nil {
				return nil, err
			}
			errCode := int16(0)
			if ver >= 2 {
				errCode, err = buf.ReadInt16()
				if err != nil {
					return nil, err
				}
			}
			out = append(out, CommittedOffset{
				Topic: topic, Partition: part, Offset: off,
				Metadata: meta, ErrorCode: errCode,
			})
		}
	}
	if ver >= 2 {
		if _, err := buf.ReadInt16(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func decodeOffsetFetchResponseFlex(body []byte) ([]CommittedOffset, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	var out []CommittedOffset
	for i := 1; i < int(nTopics); i++ {
		topic, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			off, err := buf.ReadInt64()
			if err != nil {
				return nil, err
			}
			leaderEpoch, err := buf.ReadInt32()
			_ = leaderEpoch
			if err != nil {
				return nil, err
			}
			meta, err := buf.ReadCompactNullableString()
			if err != nil {
				return nil, err
			}
			errCode, err := buf.ReadInt16()
			if err != nil {
				return nil, err
			}
			out = append(out, CommittedOffset{
				Topic: topic, Partition: part, Offset: off,
				Metadata: meta, ErrorCode: errCode,
			})
			if err := buf.SkipTagSection(); err != nil { // partition struct tag
				return nil, err
			}
		}
		if err := buf.SkipTagSection(); err != nil { // topic struct tag
			return nil, err
		}
	}
	if _, err := buf.ReadInt16(); err != nil { // group-level error_code (v2+)
		return nil, err
	}
	if err := buf.SkipTagSection(); err != nil { // response tag
		return nil, err
	}
	return out, nil
}

// EncodeOffsetFetchMultiGroupRequest encodes a batched OffsetFetch request
// (KIP-709, v8+): one request carrying several groups. partitionsByGroup maps a
// group id to the partitions to fetch; an empty partition slice for a group
// requests all of that group's committed offsets.
func EncodeOffsetFetchMultiGroupRequest(ver int16, partitionsByGroup map[string][]OffsetFetchPartition) []byte {
	if ver <= 0 {
		ver = VerOffsetFetchMultiGroup
	}
	buf := wire.NewBuffer(128)
	buf.WriteCompactArrayLen(len(partitionsByGroup))
	for group, parts := range partitionsByGroup {
		buf.WriteCompactString(group)
		if len(parts) == 0 {
			buf.WriteUvarint(0) // null topics → all topics
		} else {
			byTopic := map[string][]int32{}
			order := make([]string, 0)
			for _, p := range parts {
				if _, ok := byTopic[p.Topic]; !ok {
					order = append(order, p.Topic)
				}
				byTopic[p.Topic] = append(byTopic[p.Topic], p.Partition)
			}
			buf.WriteCompactArrayLen(len(order))
			for _, topic := range order {
				buf.WriteCompactString(topic)
				ps := byTopic[topic]
				buf.WriteCompactArrayLen(len(ps))
				for _, p := range ps {
					buf.WriteInt32(p)
				}
				buf.WriteEmptyTagSection() // topic tag
			}
		}
		buf.WriteEmptyTagSection() // group tag
	}
	buf.WriteBool(false)       // require_stable
	buf.WriteEmptyTagSection() // request tag
	return buf.Bytes()
}

// DecodeOffsetFetchMultiGroupResponse decodes a batched OffsetFetch response
// (KIP-709, v8+), returning committed offsets keyed by group id.
func DecodeOffsetFetchMultiGroupResponse(ver int16, body []byte) (map[string][]CommittedOffset, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return nil, err
	}
	nGroups, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := map[string][]CommittedOffset{}
	for g := 1; g < int(nGroups); g++ {
		group, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		nTopics, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		var offsets []CommittedOffset
		for i := 1; i < int(nTopics); i++ {
			topic, err := buf.ReadCompactString()
			if err != nil {
				return nil, err
			}
			nParts, err := buf.ReadUvarint()
			if err != nil {
				return nil, err
			}
			for j := 1; j < int(nParts); j++ {
				part, err := buf.ReadInt32()
				if err != nil {
					return nil, err
				}
				off, err := buf.ReadInt64()
				if err != nil {
					return nil, err
				}
				if _, err := buf.ReadInt32(); err != nil { // committed_leader_epoch
					return nil, err
				}
				meta, err := buf.ReadCompactNullableString()
				if err != nil {
					return nil, err
				}
				errCode, err := buf.ReadInt16()
				if err != nil {
					return nil, err
				}
				offsets = append(offsets, CommittedOffset{
					Topic: topic, Partition: part, Offset: off, Metadata: meta, ErrorCode: errCode,
				})
				if err := buf.SkipTagSection(); err != nil { // partition tag
					return nil, err
				}
			}
			if err := buf.SkipTagSection(); err != nil { // topic tag
				return nil, err
			}
		}
		if _, err := buf.ReadInt16(); err != nil { // group-level error_code
			return nil, err
		}
		if err := buf.SkipTagSection(); err != nil { // group tag
			return nil, err
		}
		out[group] = offsets
	}
	if err := buf.SkipTagSection(); err != nil { // response tag
		return nil, err
	}
	return out, nil
}

// ListOffsetsPartition requests offset for a timestamp (-2 earliest, -1 latest).
type ListOffsetsPartition struct {
	Topic     string
	Partition int32
	Timestamp int64
}

// PartitionOffset is a resolved log offset from ListOffsets.
type PartitionOffset struct {
	Topic     string
	Partition int32
	Offset    int64
	ErrorCode int16
}

func EncodeListOffsetsRequest(partitions []ListOffsetsPartition, isolation int8) []byte {
	if VerListOffsets >= 6 {
		return encodeListOffsetsRequestFlex(partitions, isolation)
	}
	return encodeListOffsetsRequestLegacy(partitions, isolation)
}

func encodeListOffsetsRequestLegacy(partitions []ListOffsetsPartition, isolation int8) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteInt32(-1)
	if VerListOffsets >= 2 {
		buf.WriteInt8(isolation)
	}
	topics := map[string][]ListOffsetsPartition{}
	order := make([]string, 0)
	for _, p := range partitions {
		if _, ok := topics[p.Topic]; !ok {
			order = append(order, p.Topic)
		}
		topics[p.Topic] = append(topics[p.Topic], p)
	}
	buf.WriteInt32(int32(len(order)))
	for _, topic := range order {
		buf.WriteString(topic)
		parts := topics[topic]
		buf.WriteInt32(int32(len(parts)))
		for _, p := range parts {
			buf.WriteInt32(p.Partition)
			buf.WriteInt64(p.Timestamp)
		}
	}
	return buf.Bytes()
}

func encodeListOffsetsRequestFlex(partitions []ListOffsetsPartition, isolation int8) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteInt32(-1)
	buf.WriteInt8(isolation)
	topics := map[string][]ListOffsetsPartition{}
	order := make([]string, 0)
	for _, p := range partitions {
		if _, ok := topics[p.Topic]; !ok {
			order = append(order, p.Topic)
		}
		topics[p.Topic] = append(topics[p.Topic], p)
	}
	buf.WriteCompactArrayLen(len(order))
	for _, topic := range order {
		buf.WriteCompactString(topic)
		parts := topics[topic]
		buf.WriteCompactArrayLen(len(parts))
		for _, p := range parts {
			buf.WriteInt32(p.Partition)
			buf.WriteInt64(p.Timestamp)
			buf.WriteEmptyTagSection()
		}
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeListOffsetsResponse(body []byte) ([]PartitionOffset, error) {
	if VerListOffsets >= 6 {
		return decodeListOffsetsResponseFlex(body)
	}
	return decodeListOffsetsResponseLegacy(body)
}

func decodeListOffsetsResponseLegacy(body []byte) ([]PartitionOffset, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	nTopics, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	var out []PartitionOffset
	for i := int32(0); i < nTopics; i++ {
		topic, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		for j := int32(0); j < nParts; j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			errCode, err := buf.ReadInt16()
			if err != nil {
				return nil, err
			}
			ts, err := buf.ReadInt64()
			_ = ts
			if err != nil {
				return nil, err
			}
			off, err := buf.ReadInt64()
			if err != nil {
				return nil, err
			}
			out = append(out, PartitionOffset{
				Topic: topic, Partition: part, Offset: off, ErrorCode: errCode,
			})
		}
	}
	return out, nil
}

func decodeListOffsetsResponseFlex(body []byte) ([]PartitionOffset, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	var out []PartitionOffset
	for i := 1; i < int(nTopics); i++ {
		topic, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			errCode, err := buf.ReadInt16()
			if err != nil {
				return nil, err
			}
			ts, err := buf.ReadInt64()
			_ = ts
			if err != nil {
				return nil, err
			}
			off, err := buf.ReadInt64()
			if err != nil {
				return nil, err
			}
			out = append(out, PartitionOffset{
				Topic: topic, Partition: part, Offset: off, ErrorCode: errCode,
			})
			if err := buf.SkipTagSection(); err != nil {
				return nil, err
			}
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

func writeInt32Array(buf *wire.Buffer, vals []int32) {
	buf.WriteInt32(int32(len(vals)))
	for _, v := range vals {
		buf.WriteInt32(v)
	}
}
