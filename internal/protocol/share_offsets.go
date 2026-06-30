package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// ShareGroupOffset is a share group's start offset (SPSO) for one partition,
// from DescribeShareGroupOffsets (API 90).
type ShareGroupOffset struct {
	Topic        string
	Partition    int32
	StartOffset  int64
	LeaderEpoch  int32
	ErrorCode    int16
	ErrorMessage string
}

// EncodeDescribeShareGroupOffsetsRequest encodes API 90 (flexible v0). It
// requests every topic-partition for one group (Topics = null).
func EncodeDescribeShareGroupOffsetsRequest(group string) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactArrayLen(1) // Groups[] — one group
	buf.WriteCompactString(group)
	buf.WriteUvarint(0)        // Topics = null → all topics with offsets
	buf.WriteEmptyTagSection() // group tag section
	buf.WriteEmptyTagSection() // request tag section
	return buf.Bytes()
}

// DecodeDescribeShareGroupOffsetsResponse decodes API 90 (flexible v0). It
// returns the first non-zero group-level error code (0 if none) and the
// per-partition start offsets across all groups in the response.
func DecodeDescribeShareGroupOffsetsResponse(body []byte) (int16, []ShareGroupOffset, error) {
	buf := wire.FromBytes(body)
	var offsets []ShareGroupOffset
	var groupErr int16
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return 0, nil, err
	}
	nGroups, err := buf.ReadUvarint()
	if err != nil {
		return 0, nil, err
	}
	for i := 1; i < int(nGroups); i++ {
		if _, err := buf.ReadCompactString(); err != nil { // GroupId
			return 0, nil, err
		}
		nTopics, err := buf.ReadUvarint()
		if err != nil {
			return 0, nil, err
		}
		for t := 1; t < int(nTopics); t++ {
			topic, err := buf.ReadCompactString()
			if err != nil {
				return 0, nil, err
			}
			if _, err := buf.ReadUUID(); err != nil { // TopicId
				return 0, nil, err
			}
			nParts, err := buf.ReadUvarint()
			if err != nil {
				return 0, nil, err
			}
			for p := 1; p < int(nParts); p++ {
				o := ShareGroupOffset{Topic: topic}
				if o.Partition, err = buf.ReadInt32(); err != nil {
					return 0, nil, err
				}
				if o.StartOffset, err = buf.ReadInt64(); err != nil {
					return 0, nil, err
				}
				if o.LeaderEpoch, err = buf.ReadInt32(); err != nil {
					return 0, nil, err
				}
				if o.ErrorCode, err = buf.ReadInt16(); err != nil {
					return 0, nil, err
				}
				if o.ErrorMessage, err = buf.ReadCompactNullableString(); err != nil {
					return 0, nil, err
				}
				if err := buf.SkipTagSection(); err != nil { // partition tag
					return 0, nil, err
				}
				offsets = append(offsets, o)
			}
			if err := buf.SkipTagSection(); err != nil { // topic tag
				return 0, nil, err
			}
		}
		gerr, err := buf.ReadInt16() // group-level error_code
		if err != nil {
			return 0, nil, err
		}
		if _, err := buf.ReadCompactNullableString(); err != nil { // group-level error_message
			return 0, nil, err
		}
		if err := buf.SkipTagSection(); err != nil { // group tag
			return 0, nil, err
		}
		if gerr != 0 && groupErr == 0 {
			groupErr = gerr
		}
	}
	if err := buf.SkipTagSection(); err != nil { // response tag
		return 0, nil, err
	}
	return groupErr, offsets, nil
}

// EncodeAlterShareGroupOffsetsRequest encodes API 91 (flexible v0): set the
// share-partition start offset for each topic-partition of one group.
func EncodeAlterShareGroupOffsetsRequest(group string, offsets map[string]map[int32]int64) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactString(group)
	buf.WriteCompactArrayLen(len(offsets))
	for topic, parts := range offsets {
		buf.WriteCompactString(topic)
		buf.WriteCompactArrayLen(len(parts))
		for p, off := range parts {
			buf.WriteInt32(p)
			buf.WriteInt64(off)
			buf.WriteEmptyTagSection() // partition tag
		}
		buf.WriteEmptyTagSection() // topic tag
	}
	buf.WriteEmptyTagSection() // request tag
	return buf.Bytes()
}

// DecodeAlterShareGroupOffsetsResponse decodes API 91 (flexible v0), returning
// the first non-zero error code (top-level or per-partition), 0 on success.
func DecodeAlterShareGroupOffsetsResponse(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return 0, err
	}
	firstErr, err := buf.ReadInt16() // top-level error_code
	if err != nil {
		return 0, err
	}
	if _, err := buf.ReadCompactNullableString(); err != nil { // top-level error_message
		return 0, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return 0, err
	}
	for t := 1; t < int(nTopics); t++ {
		if _, err := buf.ReadCompactString(); err != nil { // TopicName
			return 0, err
		}
		if _, err := buf.ReadUUID(); err != nil { // TopicId
			return 0, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return 0, err
		}
		for p := 1; p < int(nParts); p++ {
			if _, err := buf.ReadInt32(); err != nil { // PartitionIndex
				return 0, err
			}
			pErr, err := buf.ReadInt16()
			if err != nil {
				return 0, err
			}
			if _, err := buf.ReadCompactNullableString(); err != nil { // error_message
				return 0, err
			}
			if err := buf.SkipTagSection(); err != nil { // partition tag
				return 0, err
			}
			if pErr != 0 && firstErr == 0 {
				firstErr = pErr
			}
		}
		if err := buf.SkipTagSection(); err != nil { // topic tag
			return 0, err
		}
	}
	if err := buf.SkipTagSection(); err != nil { // response tag
		return 0, err
	}
	return firstErr, nil
}

// EncodeDeleteShareGroupOffsetsRequest encodes API 92 (flexible v0): delete a
// group's offsets for the given topics.
func EncodeDeleteShareGroupOffsetsRequest(group string, topics []string) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactString(group)
	buf.WriteCompactArrayLen(len(topics))
	for _, topic := range topics {
		buf.WriteCompactString(topic)
		buf.WriteEmptyTagSection() // topic tag
	}
	buf.WriteEmptyTagSection() // request tag
	return buf.Bytes()
}

// DecodeDeleteShareGroupOffsetsResponse decodes API 92 (flexible v0), returning
// the first non-zero error code (top-level or per-topic), 0 on success.
func DecodeDeleteShareGroupOffsetsResponse(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return 0, err
	}
	firstErr, err := buf.ReadInt16() // top-level error_code
	if err != nil {
		return 0, err
	}
	if _, err := buf.ReadCompactNullableString(); err != nil { // top-level error_message
		return 0, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return 0, err
	}
	for t := 1; t < int(nTopics); t++ {
		if _, err := buf.ReadCompactString(); err != nil { // TopicName
			return 0, err
		}
		if _, err := buf.ReadUUID(); err != nil { // TopicId
			return 0, err
		}
		tErr, err := buf.ReadInt16()
		if err != nil {
			return 0, err
		}
		if _, err := buf.ReadCompactNullableString(); err != nil { // error_message
			return 0, err
		}
		if err := buf.SkipTagSection(); err != nil { // topic tag
			return 0, err
		}
		if tErr != 0 && firstErr == 0 {
			firstErr = tErr
		}
	}
	if err := buf.SkipTagSection(); err != nil { // response tag
		return 0, err
	}
	return firstErr, nil
}
