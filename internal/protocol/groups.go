package protocol

import (
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

const (
	CoordinatorGroup       int8 = 0
	CoordinatorTransaction int8 = 1
)

func EncodeConsumerSubscription(topics []string, cooperative bool) []byte {
	if VerJoinGroup >= 6 {
		return encodeConsumerSubscriptionFlex(topics, cooperative)
	}
	return encodeConsumerSubscriptionLegacy(topics, cooperative)
}

func encodeConsumerSubscriptionLegacy(topics []string, cooperative bool) []byte {
	buf := wire.NewBuffer(64)
	if cooperative {
		buf.WriteInt16(3)
	} else {
		buf.WriteInt16(0)
	}
	buf.WriteInt32(int32(len(topics)))
	for _, t := range topics {
		buf.WriteString(t)
	}
	buf.WriteBytes(nil)
	return buf.Bytes()
}

func encodeConsumerSubscriptionFlex(topics []string, cooperative bool) []byte {
	buf := wire.NewBuffer(64)
	if cooperative {
		buf.WriteInt16(3) // cooperative-sticky subscription version
	} else {
		buf.WriteInt16(0)
	}
	buf.WriteCompactArrayLen(len(topics))
	for _, t := range topics {
		buf.WriteCompactString(t)
	}
	buf.WriteCompactBytes(nil)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func EncodeJoinGroupRequest(group, memberID, assignor, instanceID string, topics []string, sessionTimeoutMs, rebalanceTimeoutMs int32, cooperative bool) []byte {
	if VerJoinGroup >= 6 {
		return encodeJoinGroupRequestFlex(group, memberID, assignor, instanceID, topics, sessionTimeoutMs, rebalanceTimeoutMs, cooperative)
	}
	return encodeJoinGroupRequestLegacy(group, memberID, assignor, instanceID, topics, sessionTimeoutMs, rebalanceTimeoutMs, cooperative)
}

func encodeJoinGroupRequestLegacy(group, memberID, assignor, instanceID string, topics []string, sessionTimeoutMs, rebalanceTimeoutMs int32, cooperative bool) []byte {
	if assignor == "" {
		assignor = "range"
	}
	if sessionTimeoutMs <= 0 {
		sessionTimeoutMs = 45000
	}
	if rebalanceTimeoutMs <= 0 {
		rebalanceTimeoutMs = sessionTimeoutMs
	}
	meta := EncodeConsumerSubscription(topics, cooperative)

	buf := wire.NewBuffer(128)
	buf.WriteString(group)
	buf.WriteInt32(sessionTimeoutMs)
	buf.WriteInt32(rebalanceTimeoutMs)
	buf.WriteString(memberID)
	if VerJoinGroup >= 5 && instanceID != "" {
		buf.WriteNullableString(&instanceID)
	} else if VerJoinGroup >= 5 {
		buf.WriteInt16(-1)
	}
	consumerType := "consumer"
	buf.WriteString(consumerType)
	buf.WriteInt32(1)
	buf.WriteString(assignor)
	buf.WriteBytes(meta)
	return buf.Bytes()
}

func encodeJoinGroupRequestFlex(group, memberID, assignor, instanceID string, topics []string, sessionTimeoutMs, rebalanceTimeoutMs int32, cooperative bool) []byte {
	if assignor == "" {
		assignor = "range"
	}
	if sessionTimeoutMs <= 0 {
		sessionTimeoutMs = 45000
	}
	if rebalanceTimeoutMs <= 0 {
		rebalanceTimeoutMs = sessionTimeoutMs
	}
	meta := EncodeConsumerSubscription(topics, cooperative)

	buf := wire.NewBuffer(128)
	buf.WriteCompactString(group)
	buf.WriteInt32(sessionTimeoutMs)
	buf.WriteInt32(rebalanceTimeoutMs)
	buf.WriteCompactString(memberID)
	if instanceID != "" {
		buf.WriteCompactString(instanceID)
	} else {
		buf.WriteCompactNullableString(nil)
	}
	consumerType := "consumer"
	buf.WriteCompactNullableString(&consumerType)
	buf.WriteCompactArrayLen(1)
	buf.WriteCompactString(assignor)
	buf.WriteCompactBytes(meta)
	buf.WriteEmptyTagSection()
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

type JoinGroupResponse struct {
	GenerationID int32
	MemberID     string
	LeaderID     string
	Protocol     string
	Assignments  map[string][]byte
}

func DecodeJoinGroupResponse(body []byte) (JoinGroupResponse, error) {
	if VerJoinGroup >= 6 {
		return decodeJoinGroupResponseFlex(body)
	}
	return decodeJoinGroupResponseLegacy(body)
}

func decodeJoinGroupResponseLegacy(body []byte) (JoinGroupResponse, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return JoinGroupResponse{}, err
	}
	errCode, err := buf.ReadInt16()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	if errCode != 0 && errCode != ErrorCodeMemberIDRequired {
		return JoinGroupResponse{}, apiError("join group", errCode)
	}
	gen, err := buf.ReadInt32()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	protocolName, err := buf.ReadString()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	leader, err := buf.ReadString()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	memberID, err := buf.ReadString()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	nMembers, err := buf.ReadInt32()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	assignments := map[string][]byte{}
	for i := int32(0); i < nMembers; i++ {
		mid, err := buf.ReadString()
		if err != nil {
			return JoinGroupResponse{}, err
		}
		if VerJoinGroup >= 5 {
			if _, err := readNullableString(buf); err != nil {
				return JoinGroupResponse{}, err
			}
		}
		meta, err := buf.ReadBytes()
		if err != nil {
			return JoinGroupResponse{}, err
		}
		assignments[mid] = meta
	}
	result := JoinGroupResponse{GenerationID: gen, MemberID: memberID, LeaderID: leader, Protocol: protocolName, Assignments: assignments}
	if errCode == ErrorCodeMemberIDRequired {
		return result, ErrMemberIDRequired
	}
	return result, nil
}

func decodeJoinGroupResponseFlex(body []byte) (JoinGroupResponse, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return JoinGroupResponse{}, err
	}
	errCode, err := buf.ReadInt16()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	if errCode != 0 && errCode != ErrorCodeMemberIDRequired {
		return JoinGroupResponse{}, apiError("join group", errCode)
	}
	gen, err := buf.ReadInt32()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	protocol, err := buf.ReadCompactString()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	leader, err := buf.ReadCompactString()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	memberID, err := buf.ReadCompactString()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	nMembers, err := buf.ReadUvarint()
	if err != nil {
		return JoinGroupResponse{}, err
	}
	assignments := map[string][]byte{}
	for i := 1; i < int(nMembers); i++ {
		mid, err := buf.ReadCompactString()
		if err != nil {
			return JoinGroupResponse{}, err
		}
		meta, err := buf.ReadCompactBytes()
		if err != nil {
			return JoinGroupResponse{}, err
		}
		assignments[mid] = meta
		if err := buf.SkipTagSection(); err != nil {
			return JoinGroupResponse{}, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return JoinGroupResponse{}, err
	}
	result := JoinGroupResponse{GenerationID: gen, MemberID: memberID, LeaderID: leader, Protocol: protocol, Assignments: assignments}
	if errCode == ErrorCodeMemberIDRequired {
		return result, ErrMemberIDRequired
	}
	return result, nil
}

func EncodeSyncGroupRequest(group, memberID, protocol string, generation int32, assignments map[string][]byte) []byte {
	if VerSyncGroup >= 4 {
		return encodeSyncGroupRequestFlex(group, memberID, protocol, generation, assignments)
	}
	return encodeSyncGroupRequestLegacy(group, memberID, protocol, generation, assignments)
}

func encodeSyncGroupRequestLegacy(group, memberID, protocol string, generation int32, assignments map[string][]byte) []byte {
	buf := wire.NewBuffer(128)
	buf.WriteString(group)
	buf.WriteInt32(generation)
	buf.WriteString(memberID)
	buf.WriteString(protocol)
	buf.WriteInt32(int32(len(assignments)))
	for mid, meta := range assignments {
		buf.WriteString(mid)
		buf.WriteBytes(meta)
	}
	return buf.Bytes()
}

func encodeSyncGroupRequestFlex(group, memberID, protocol string, generation int32, assignments map[string][]byte) []byte {
	buf := wire.NewBuffer(128)
	buf.WriteCompactString(group)
	buf.WriteInt32(generation)
	buf.WriteCompactString(memberID)
	buf.WriteCompactString(protocol)
	buf.WriteCompactArrayLen(len(assignments))
	for mid, meta := range assignments {
		buf.WriteCompactString(mid)
		buf.WriteCompactBytes(meta)
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeSyncGroupResponse(body []byte) ([]byte, error) {
	if VerSyncGroup >= 4 {
		return decodeSyncGroupResponseFlex(body)
	}
	return decodeSyncGroupResponseLegacy(body)
}

func decodeSyncGroupResponseLegacy(body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	errCode, err := buf.ReadInt16()
	if err != nil {
		return nil, err
	}
	if errCode != 0 {
		return nil, apiError("sync group", errCode)
	}
	return buf.ReadBytes()
}

func decodeSyncGroupResponseFlex(body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	errCode, err := buf.ReadInt16()
	if err != nil {
		return nil, err
	}
	if errCode != 0 {
		return nil, apiError("sync group", errCode)
	}
	assignment, err := buf.ReadCompactBytes()
	if err != nil {
		return nil, err
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return assignment, nil
}

func EncodeHeartbeatRequest(group, memberID string, generation int32) []byte {
	if VerHeartbeat >= 4 {
		buf := wire.NewBuffer(32)
		buf.WriteCompactString(group)
		buf.WriteInt32(generation)
		buf.WriteCompactString(memberID)
		buf.WriteEmptyTagSection()
		return buf.Bytes()
	}
	buf := wire.NewBuffer(32)
	buf.WriteString(group)
	buf.WriteInt32(generation)
	buf.WriteString(memberID)
	return buf.Bytes()
}

// DecodeHeartbeatResponse reads the top-level error code from Heartbeat.
func DecodeHeartbeatResponse(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	code, err := buf.ReadInt16()
	if err != nil {
		return 0, err
	}
	if VerHeartbeat >= 4 {
		if err := buf.SkipTagSection(); err != nil {
			return 0, err
		}
	}
	return code, nil
}

func EncodeLeaveGroupRequest(group, memberID string) []byte {
	if VerLeaveGroup >= 4 {
		buf := wire.NewBuffer(16)
		buf.WriteCompactString(group)
		buf.WriteCompactArrayLen(1)
		buf.WriteCompactString(memberID)
		buf.WriteEmptyTagSection()
		buf.WriteEmptyTagSection()
		return buf.Bytes()
	}
	buf := wire.NewBuffer(16)
	buf.WriteString(group)
	buf.WriteString(memberID)
	return buf.Bytes()
}

func EncodeOffsetCommitRequest(ver int16, group, memberID, groupInstanceID string, generation int32, offsets map[string]map[int32]int64) []byte {
	if ver <= 0 {
		ver = VerOffsetCommit
	}
	if ver >= 8 {
		return encodeOffsetCommitRequestFlex(ver, group, memberID, groupInstanceID, generation, offsets)
	}
	return encodeOffsetCommitRequestLegacy(ver, group, memberID, groupInstanceID, generation, offsets)
}

func encodeOffsetCommitRequestLegacy(ver int16, group, memberID, groupInstanceID string, generation int32, offsets map[string]map[int32]int64) []byte {
	buf := wire.NewBuffer(128)
	buf.WriteString(group)
	if ver >= 1 {
		buf.WriteInt32(generation)
		buf.WriteString(memberID)
	}
	if ver >= 7 {
		if groupInstanceID == "" {
			buf.WriteNullableString(nil)
		} else {
			s := groupInstanceID
			buf.WriteNullableString(&s)
		}
	}
	if ver >= 2 && ver <= 4 {
		buf.WriteInt64(-1)
	}
	buf.WriteInt32(int32(len(offsets)))
	for topic, parts := range offsets {
		buf.WriteString(topic)
		buf.WriteInt32(int32(len(parts)))
		for part, off := range parts {
			buf.WriteInt32(part)
			buf.WriteInt64(off)
			if ver >= 6 {
				buf.WriteInt32(-1)
			}
			if ver == 1 {
				buf.WriteInt64(-1)
			}
			buf.WriteNullableString(nil)
		}
	}
	return buf.Bytes()
}

func encodeOffsetCommitRequestFlex(ver int16, group, memberID, groupInstanceID string, generation int32, offsets map[string]map[int32]int64) []byte {
	buf := wire.NewBuffer(128)
	buf.WriteCompactString(group)
	buf.WriteInt32(generation)
	buf.WriteCompactString(memberID)
	if ver >= 7 {
		if groupInstanceID == "" {
			buf.WriteUvarint(0)
		} else {
			buf.WriteCompactString(groupInstanceID)
		}
	}
	buf.WriteCompactArrayLen(len(offsets))
	for topic, parts := range offsets {
		buf.WriteCompactString(topic)
		buf.WriteCompactArrayLen(len(parts))
		for part, off := range parts {
			buf.WriteInt32(part)
			buf.WriteInt64(off)
			if ver >= 6 {
				buf.WriteInt32(-1)
			}
			buf.WriteCompactNullableString(nil)
			buf.WriteEmptyTagSection()
		}
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func EncodeCreateTopicsRequest(topics map[string]TopicCreate) []byte {
	if VerCreateTopics >= 5 {
		return encodeCreateTopicsRequestFlex(topics)
	}
	return encodeCreateTopicsRequestLegacy(topics)
}

func encodeCreateTopicsRequestLegacy(topics map[string]TopicCreate) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteInt32(int32(len(topics)))
	for name, cfg := range topics {
		buf.WriteString(name)
		buf.WriteInt32(cfg.Partitions)
		buf.WriteInt16(cfg.ReplicationFactor)
		buf.WriteInt32(0) // assignments
		buf.WriteInt32(int32(len(cfg.Configs)))
		for k, v := range cfg.Configs {
			buf.WriteString(k)
			buf.WriteString(v)
		}
	}
	buf.WriteInt32(5000)
	buf.WriteBool(false)
	return buf.Bytes()
}

func encodeCreateTopicsRequestFlex(topics map[string]TopicCreate) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactArrayLen(len(topics))
	for name, cfg := range topics {
		buf.WriteCompactString(name)
		buf.WriteInt32(cfg.Partitions)
		buf.WriteInt16(cfg.ReplicationFactor)
		buf.WriteCompactArrayLen(0)
		buf.WriteCompactArrayLen(len(cfg.Configs))
		for k, v := range cfg.Configs {
			buf.WriteCompactString(k)
			buf.WriteCompactString(v)
			buf.WriteEmptyTagSection()
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteInt32(5000)
	buf.WriteBool(false)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeOffsetCommitResponse returns the first non-zero partition error code, if any.
func DecodeOffsetCommitResponse(ver int16, body []byte) (int16, error) {
	if ver >= 8 {
		return decodeOffsetCommitResponseFlex(body)
	}
	code, err := decodeOffsetCommitResponseLegacy(ver, body)
	if err != nil {
		return 0, err
	}
	if code == 0 {
		return 0, nil
	}
	if alt, err2 := decodeOffsetCommitResponseFlex(body); err2 == nil {
		return alt, nil
	}
	return code, nil
}

func decodeOffsetCommitResponseLegacy(ver int16, body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if ver >= 3 {
		if _, err := buf.ReadInt32(); err != nil { // throttle
			return 0, err
		}
	}
	nTopics, err := buf.ReadInt32()
	if err != nil {
		return 0, err
	}
	for i := int32(0); i < nTopics; i++ {
		if _, err := buf.ReadString(); err != nil {
			return 0, err
		}
		nParts, err := buf.ReadInt32()
		if err != nil {
			return 0, err
		}
		for j := int32(0); j < nParts; j++ {
			if _, err := buf.ReadInt32(); err != nil {
				return 0, err
			}
			code, err := buf.ReadInt16()
			if err != nil {
				return 0, err
			}
			if code != 0 {
				return code, nil
			}
		}
	}
	return 0, nil
}

func decodeOffsetCommitResponseFlex(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return 0, err
	}
	for i := 1; i < int(nTopics); i++ {
		if _, err := buf.ReadCompactString(); err != nil {
			return 0, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return 0, err
		}
		for j := 1; j < int(nParts); j++ {
			if _, err := buf.ReadInt32(); err != nil {
				return 0, err
			}
			code, err := buf.ReadInt16()
			if err != nil {
				return 0, err
			}
			if code != 0 {
				return code, nil
			}
			if err := buf.SkipTagSection(); err != nil {
				return 0, err
			}
		}
		if err := buf.SkipTagSection(); err != nil {
			return 0, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return 0, err
	}
	return 0, nil
}

type TopicCreate struct {
	Partitions        int32
	ReplicationFactor int16
	Configs           map[string]string
}

func EncodeDeleteTopicsRequest(topics []string) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteCompactArrayLen(len(topics))
	for _, t := range topics {
		buf.WriteCompactString(t)
	}
	buf.WriteInt32(5000)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}
