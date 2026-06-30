package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// ReassignmentMove is a requested partition replica reassignment. Nil Replicas
// cancels an in-progress reassignment for the partition.
type ReassignmentMove struct {
	Partition int32
	Replicas  []int32 // nil = cancel
}

// PartitionReassignmentResult is the per-partition outcome of AlterPartitionReassignments.
type PartitionReassignmentResult struct {
	Topic        string
	Partition    int32
	ErrorCode    int16
	ErrorMessage string
}

// EncodeAlterPartitionReassignmentsRequest encodes API 45 (flexible v0).
func EncodeAlterPartitionReassignmentsRequest(timeoutMs int32, topics map[string][]ReassignmentMove) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteInt32(timeoutMs)
	buf.WriteCompactArrayLen(len(topics))
	for name, moves := range topics {
		buf.WriteCompactString(name)
		buf.WriteCompactArrayLen(len(moves))
		for _, m := range moves {
			buf.WriteInt32(m.Partition)
			if m.Replicas == nil {
				buf.WriteUvarint(0) // null replicas = cancel
			} else {
				buf.WriteCompactArrayLen(len(m.Replicas))
				for _, r := range m.Replicas {
					buf.WriteInt32(r)
				}
			}
			buf.WriteEmptyTagSection()
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeAlterPartitionReassignmentsResponse decodes API 45 response (flexible v0).
func DecodeAlterPartitionReassignmentsResponse(body []byte) (int16, string, []PartitionReassignmentResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return 0, "", nil, err
	}
	topErr, err := buf.ReadInt16()
	if err != nil {
		return 0, "", nil, err
	}
	topMsg, err := buf.ReadCompactNullableString()
	if err != nil {
		return topErr, "", nil, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return topErr, topMsg, nil, err
	}
	var out []PartitionReassignmentResult
	for i := 1; i < int(nTopics); i++ {
		name, err := buf.ReadCompactString()
		if err != nil {
			return topErr, topMsg, nil, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return topErr, topMsg, nil, err
		}
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return topErr, topMsg, nil, err
			}
			code, err := buf.ReadInt16()
			if err != nil {
				return topErr, topMsg, nil, err
			}
			msg, err := buf.ReadCompactNullableString()
			if err != nil {
				return topErr, topMsg, nil, err
			}
			if err := buf.SkipTagSection(); err != nil {
				return topErr, topMsg, nil, err
			}
			out = append(out, PartitionReassignmentResult{Topic: name, Partition: part, ErrorCode: code, ErrorMessage: msg})
		}
		if err := buf.SkipTagSection(); err != nil {
			return topErr, topMsg, nil, err
		}
	}
	return topErr, topMsg, out, nil
}

// OngoingReassignment describes an in-progress partition reassignment.
type OngoingReassignment struct {
	Topic            string
	Partition        int32
	Replicas         []int32
	AddingReplicas   []int32
	RemovingReplicas []int32
}

// EncodeListPartitionReassignmentsRequest encodes API 46 (flexible v0). Nil
// topics lists all ongoing reassignments.
func EncodeListPartitionReassignmentsRequest(timeoutMs int32, topics map[string][]int32) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteInt32(timeoutMs)
	if len(topics) == 0 {
		buf.WriteUvarint(0) // null topics = all
	} else {
		buf.WriteCompactArrayLen(len(topics))
		for name, parts := range topics {
			buf.WriteCompactString(name)
			buf.WriteCompactArrayLen(len(parts))
			for _, p := range parts {
				buf.WriteInt32(p)
			}
			buf.WriteEmptyTagSection()
		}
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeListPartitionReassignmentsResponse decodes API 46 response (flexible v0).
func DecodeListPartitionReassignmentsResponse(body []byte) (int16, string, []OngoingReassignment, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return 0, "", nil, err
	}
	topErr, err := buf.ReadInt16()
	if err != nil {
		return 0, "", nil, err
	}
	topMsg, err := buf.ReadCompactNullableString()
	if err != nil {
		return topErr, "", nil, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return topErr, topMsg, nil, err
	}
	var out []OngoingReassignment
	for i := 1; i < int(nTopics); i++ {
		name, err := buf.ReadCompactString()
		if err != nil {
			return topErr, topMsg, nil, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return topErr, topMsg, nil, err
		}
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return topErr, topMsg, nil, err
			}
			replicas, err := readCompactInt32Array(buf)
			if err != nil {
				return topErr, topMsg, nil, err
			}
			adding, err := readCompactInt32Array(buf)
			if err != nil {
				return topErr, topMsg, nil, err
			}
			removing, err := readCompactInt32Array(buf)
			if err != nil {
				return topErr, topMsg, nil, err
			}
			if err := buf.SkipTagSection(); err != nil {
				return topErr, topMsg, nil, err
			}
			out = append(out, OngoingReassignment{
				Topic: name, Partition: part,
				Replicas: replicas, AddingReplicas: adding, RemovingReplicas: removing,
			})
		}
		if err := buf.SkipTagSection(); err != nil {
			return topErr, topMsg, nil, err
		}
	}
	return topErr, topMsg, out, nil
}
