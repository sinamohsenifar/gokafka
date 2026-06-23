package protocol

import (
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// TopicIDPartitions maps a topic UUID to partition indexes (KIP-848).
type TopicIDPartitions struct {
	TopicID    wire.UUID
	Partitions []int32
}

// ConsumerGroupHeartbeatRequest is a KIP-848 heartbeat (server-side assignor mode).
type ConsumerGroupHeartbeatRequest struct {
	GroupID              string
	MemberID             string
	MemberEpoch          int32
	InstanceID           *string
	RackID               *string
	RebalanceTimeoutMs   int32 // -1 when unchanged
	SubscribedTopicNames []string
	SubscribedTopicRegex *string
	ServerAssignor       *string
	TopicPartitions      []TopicIDPartitions
}

// ConsumerGroupHeartbeatResponse is the KIP-848 heartbeat response.
type ConsumerGroupHeartbeatResponse struct {
	ThrottleTimeMs      int32
	ErrorCode           int16
	ErrorMessage        string
	MemberID            string
	MemberEpoch         int32
	HeartbeatIntervalMs int32
	Assignment          []TopicIDPartitions
}

// EncodeConsumerGroupHeartbeatRequest encodes API 68 (flex v1).
func EncodeConsumerGroupHeartbeatRequest(req ConsumerGroupHeartbeatRequest) []byte {
	buf := wire.NewBuffer(256)
	buf.WriteCompactString(req.GroupID)
	buf.WriteCompactString(req.MemberID)
	buf.WriteInt32(req.MemberEpoch)
	buf.WriteCompactNullableString(req.InstanceID)
	buf.WriteCompactNullableString(req.RackID)
	buf.WriteInt32(req.RebalanceTimeoutMs)
	writeCompactNullableStringArray(buf, req.SubscribedTopicNames)
	buf.WriteCompactNullableString(req.SubscribedTopicRegex)
	buf.WriteCompactNullableString(req.ServerAssignor)
	writeCompactNullableTopicIDPartitions(buf, req.TopicPartitions)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func writeCompactNullableStringArray(buf *wire.Buffer, topics []string) {
	if topics == nil {
		buf.WriteUvarint(0)
		return
	}
	buf.WriteCompactArrayLen(len(topics))
	for _, t := range topics {
		buf.WriteCompactString(t)
	}
}

func writeCompactNullableTopicIDPartitions(buf *wire.Buffer, parts []TopicIDPartitions) {
	if parts == nil {
		buf.WriteUvarint(0)
		return
	}
	buf.WriteCompactArrayLen(len(parts))
	for _, tp := range parts {
		buf.WriteUUID(tp.TopicID)
		buf.WriteCompactArrayLen(len(tp.Partitions))
		for _, p := range tp.Partitions {
			buf.WriteInt32(p)
		}
	}
}

// DecodeConsumerGroupHeartbeatResponse decodes API 68 flex response (v0/v1).
func DecodeConsumerGroupHeartbeatResponse(body []byte) (ConsumerGroupHeartbeatResponse, error) {
	buf := wire.FromBytes(body)
	var out ConsumerGroupHeartbeatResponse
	var err error
	if out.ThrottleTimeMs, err = buf.ReadInt32(); err != nil {
		return out, err
	}
	if out.ErrorCode, err = buf.ReadInt16(); err != nil {
		return out, err
	}
	if out.ErrorMessage, err = buf.ReadCompactNullableString(); err != nil {
		return out, err
	}
	if out.MemberID, err = buf.ReadCompactNullableString(); err != nil {
		return out, err
	}
	if out.MemberEpoch, err = buf.ReadInt32(); err != nil {
		return out, err
	}
	if out.HeartbeatIntervalMs, err = buf.ReadInt32(); err != nil {
		return out, err
	}
	assignMark, err := buf.ReadUvarint()
	if err != nil {
		return out, err
	}
	if assignMark != 0 {
		nTP, err := buf.ReadUvarint()
		if err != nil {
			return out, err
		}
		for i := 1; i < int(nTP); i++ {
			tp, err := readTopicIDPartitions(buf)
			if err != nil {
				return out, err
			}
			out.Assignment = append(out.Assignment, tp)
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return out, err
	}
	if out.ErrorCode != 0 {
		return out, apiError("consumer group heartbeat", out.ErrorCode)
	}
	return out, nil
}

func readTopicIDPartitions(buf *wire.Buffer) (TopicIDPartitions, error) {
	var tp TopicIDPartitions
	var err error
	if tp.TopicID, err = buf.ReadUUID(); err != nil {
		return tp, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return tp, err
	}
	for i := 1; i < int(n); i++ {
		p, err := buf.ReadInt32()
		if err != nil {
			return tp, err
		}
		tp.Partitions = append(tp.Partitions, p)
	}
	return tp, nil
}

// DecodeConsumerGroupHeartbeatResponseLegacy handles v0 Assignment wrapper (ShouldComputeAssignment + nested fields).
func DecodeConsumerGroupHeartbeatResponseLegacy(body []byte) (ConsumerGroupHeartbeatResponse, error) {
	buf := wire.FromBytes(body)
	var out ConsumerGroupHeartbeatResponse
	var err error
	if out.ThrottleTimeMs, err = buf.ReadInt32(); err != nil {
		return out, err
	}
	if out.ErrorCode, err = buf.ReadInt16(); err != nil {
		return out, err
	}
	if out.ErrorMessage, err = buf.ReadCompactNullableString(); err != nil {
		return out, err
	}
	if out.MemberID, err = buf.ReadCompactNullableString(); err != nil {
		return out, err
	}
	if out.MemberEpoch, err = buf.ReadInt32(); err != nil {
		return out, err
	}
	if _, err = buf.ReadBool(); err != nil { // ShouldComputeAssignment (v0)
		return out, err
	}
	if out.HeartbeatIntervalMs, err = buf.ReadInt32(); err != nil {
		return out, err
	}
	assignLen, err := buf.ReadUvarint()
	if err != nil {
		return out, err
	}
	if assignLen > 1 {
		if _, err = buf.ReadInt8(); err != nil { // assignment error
			return out, err
		}
		nTP, err := buf.ReadUvarint()
		if err != nil {
			return out, err
		}
		for i := 1; i < int(nTP); i++ {
			tp, err := readTopicIDPartitions(buf)
			if err != nil {
				return out, err
			}
			out.Assignment = append(out.Assignment, tp)
		}
		_, _ = buf.ReadUvarint() // pending topic partitions
		_, _ = buf.ReadInt16()   // metadata version
		_, _ = buf.ReadCompactBytes()
	}
	if err := buf.SkipTagSection(); err != nil {
		return out, err
	}
	if out.ErrorCode != 0 {
		return out, apiError("consumer group heartbeat", out.ErrorCode)
	}
	return out, nil
}
