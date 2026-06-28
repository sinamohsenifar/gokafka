package protocol

import (
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// ShareGroupHeartbeatRequest is KIP-932 share group join/heartbeat (API 76 v1).
type ShareGroupHeartbeatRequest struct {
	GroupID              string
	MemberID             string
	MemberEpoch          int32
	RackID               *string
	SubscribedTopicNames []string // nil when unchanged after join
}

// ShareGroupHeartbeatResponse is the KIP-932 heartbeat response.
type ShareGroupHeartbeatResponse struct {
	ThrottleTimeMs      int32
	ErrorCode           int16
	ErrorMessage        string
	MemberID            string
	MemberEpoch         int32
	HeartbeatIntervalMs int32
	Assignment          []TopicIDPartitions
}

// EncodeShareGroupHeartbeatRequest encodes API 76 flex v1.
func EncodeShareGroupHeartbeatRequest(req ShareGroupHeartbeatRequest) []byte {
	buf := wire.NewBuffer(256)
	buf.WriteCompactString(req.GroupID)
	buf.WriteCompactString(req.MemberID)
	buf.WriteInt32(req.MemberEpoch)
	buf.WriteCompactNullableString(req.RackID)
	writeCompactNullableStringArray(buf, req.SubscribedTopicNames)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeShareGroupHeartbeatResponse decodes API 76 flex response v1.
func DecodeShareGroupHeartbeatResponse(body []byte) (ShareGroupHeartbeatResponse, error) {
	buf := wire.FromBytes(body)
	var out ShareGroupHeartbeatResponse
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
	presence, err := buf.ReadInt8()
	if err != nil {
		return out, err
	}
	if presence >= 0 {
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
		if err := buf.SkipTagSection(); err != nil {
			return out, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return out, err
	}
	if out.ErrorCode != 0 {
		return out, apiError("share group heartbeat", out.ErrorCode)
	}
	return out, nil
}
