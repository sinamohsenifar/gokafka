package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// DeleteRecordsResult is a per-partition result of a DeleteRecords request.
type DeleteRecordsResult struct {
	Topic        string
	Partition    int32
	LowWatermark int64
	ErrorCode    int16
}

// EncodeDeleteRecordsRequest encodes API 21 (DeleteRecords). offsets maps a
// topic to a partition->delete-before-offset map (-1 means high watermark).
func EncodeDeleteRecordsRequest(version int16, offsets map[string]map[int32]int64, timeoutMs int32) []byte {
	if version >= 2 {
		buf := wire.NewBuffer(64)
		buf.WriteCompactArrayLen(len(offsets))
		for topic, parts := range offsets {
			buf.WriteCompactString(topic)
			buf.WriteCompactArrayLen(len(parts))
			for p, off := range parts {
				buf.WriteInt32(p)
				buf.WriteInt64(off)
				buf.WriteEmptyTagSection()
			}
			buf.WriteEmptyTagSection()
		}
		buf.WriteInt32(timeoutMs)
		buf.WriteEmptyTagSection()
		return buf.Bytes()
	}
	buf := wire.NewBuffer(64)
	buf.WriteInt32(int32(len(offsets)))
	for topic, parts := range offsets {
		buf.WriteString(topic)
		buf.WriteInt32(int32(len(parts)))
		for p, off := range parts {
			buf.WriteInt32(p)
			buf.WriteInt64(off)
		}
	}
	buf.WriteInt32(timeoutMs)
	return buf.Bytes()
}

// DecodeDeleteRecordsResponse decodes API 21 response.
func DecodeDeleteRecordsResponse(version int16, body []byte) ([]DeleteRecordsResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return nil, err
	}
	var out []DeleteRecordsResult
	if version >= 2 {
		nTopics, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
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
				r := DeleteRecordsResult{Topic: topic}
				if r.Partition, err = buf.ReadInt32(); err != nil {
					return nil, err
				}
				if r.LowWatermark, err = buf.ReadInt64(); err != nil {
					return nil, err
				}
				if r.ErrorCode, err = buf.ReadInt16(); err != nil {
					return nil, err
				}
				if err := buf.SkipTagSection(); err != nil {
					return nil, err
				}
				out = append(out, r)
			}
			if err := buf.SkipTagSection(); err != nil {
				return nil, err
			}
		}
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
		return out, nil
	}
	nTopics, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
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
			r := DeleteRecordsResult{Topic: topic}
			if r.Partition, err = buf.ReadInt32(); err != nil {
				return nil, err
			}
			if r.LowWatermark, err = buf.ReadInt64(); err != nil {
				return nil, err
			}
			if r.ErrorCode, err = buf.ReadInt16(); err != nil {
				return nil, err
			}
			out = append(out, r)
		}
	}
	return out, nil
}

// ElectLeadersResult is a per-partition result of an ElectLeaders request.
type ElectLeadersResult struct {
	Topic        string
	Partition    int32
	ErrorCode    int16
	ErrorMessage string
}

// Election types for ElectLeaders.
const (
	ElectionPreferred int8 = 0
	ElectionUnclean   int8 = 1
)

// EncodeElectLeadersRequest encodes API 43 (ElectLeaders). A nil topicPartitions
// means "all partitions".
func EncodeElectLeadersRequest(version int16, electionType int8, topicPartitions map[string][]int32, timeoutMs int32) []byte {
	if version >= 2 {
		buf := wire.NewBuffer(64)
		buf.WriteInt8(electionType)
		if topicPartitions == nil {
			buf.WriteCompactArrayLen(-1) // null => all
		} else {
			buf.WriteCompactArrayLen(len(topicPartitions))
			for topic, parts := range topicPartitions {
				buf.WriteCompactString(topic)
				buf.WriteCompactArrayLen(len(parts))
				for _, p := range parts {
					buf.WriteInt32(p)
				}
				buf.WriteEmptyTagSection()
			}
		}
		buf.WriteInt32(timeoutMs)
		buf.WriteEmptyTagSection()
		return buf.Bytes()
	}
	buf := wire.NewBuffer(64)
	if version >= 1 {
		buf.WriteInt8(electionType)
	}
	if topicPartitions == nil {
		buf.WriteInt32(-1)
	} else {
		buf.WriteInt32(int32(len(topicPartitions)))
		for topic, parts := range topicPartitions {
			buf.WriteString(topic)
			buf.WriteInt32(int32(len(parts)))
			for _, p := range parts {
				buf.WriteInt32(p)
			}
		}
	}
	buf.WriteInt32(timeoutMs)
	return buf.Bytes()
}

// DecodeElectLeadersResponse decodes API 43 response. The returned code is the
// top-level error (v1+); 0 on v0.
func DecodeElectLeadersResponse(version int16, body []byte) (int16, []ElectLeadersResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return 0, nil, err
	}
	var topErr int16
	if version >= 1 {
		var err error
		if topErr, err = buf.ReadInt16(); err != nil {
			return 0, nil, err
		}
	}
	var out []ElectLeadersResult
	if version >= 2 {
		nTopics, err := buf.ReadUvarint()
		if err != nil {
			return topErr, nil, err
		}
		for i := 1; i < int(nTopics); i++ {
			topic, err := buf.ReadCompactString()
			if err != nil {
				return topErr, nil, err
			}
			nParts, err := buf.ReadUvarint()
			if err != nil {
				return topErr, nil, err
			}
			for j := 1; j < int(nParts); j++ {
				r := ElectLeadersResult{Topic: topic}
				if r.Partition, err = buf.ReadInt32(); err != nil {
					return topErr, nil, err
				}
				if r.ErrorCode, err = buf.ReadInt16(); err != nil {
					return topErr, nil, err
				}
				if r.ErrorMessage, err = buf.ReadCompactNullableString(); err != nil {
					return topErr, nil, err
				}
				if err := buf.SkipTagSection(); err != nil {
					return topErr, nil, err
				}
				out = append(out, r)
			}
			if err := buf.SkipTagSection(); err != nil {
				return topErr, nil, err
			}
		}
		if err := buf.SkipTagSection(); err != nil {
			return topErr, nil, err
		}
		return topErr, out, nil
	}
	nTopics, err := buf.ReadInt32()
	if err != nil {
		return topErr, nil, err
	}
	for i := int32(0); i < nTopics; i++ {
		topic, err := buf.ReadString()
		if err != nil {
			return topErr, nil, err
		}
		nParts, err := buf.ReadInt32()
		if err != nil {
			return topErr, nil, err
		}
		for j := int32(0); j < nParts; j++ {
			r := ElectLeadersResult{Topic: topic}
			if r.Partition, err = buf.ReadInt32(); err != nil {
				return topErr, nil, err
			}
			if r.ErrorCode, err = buf.ReadInt16(); err != nil {
				return topErr, nil, err
			}
			if r.ErrorMessage, err = readNullableString(buf); err != nil {
				return topErr, nil, err
			}
			out = append(out, r)
		}
	}
	return topErr, out, nil
}
