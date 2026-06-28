package protocol

import (
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// ShareAckType is a KIP-932 delivery acknowledgement.
type ShareAckType int8

const (
	ShareAckGap     ShareAckType = 0
	ShareAckAccept  ShareAckType = 1
	ShareAckRelease ShareAckType = 2
	ShareAckReject  ShareAckType = 3
	ShareAckRenew   ShareAckType = 4
)

// ShareAckBatch acknowledges a contiguous offset range.
type ShareAckBatch struct {
	FirstOffset int64
	LastOffset  int64
	Type        ShareAckType
}

// ShareFetchPartition is a topic-partition in ShareFetch.
type ShareFetchPartition struct {
	TopicID    wire.UUID
	Partition  int32
	AckBatches []ShareAckBatch
}

// ShareFetchRequest is KIP-932 ShareFetch (API 78 v1+).
type ShareFetchRequest struct {
	GroupID           string
	MemberID          string
	ShareSessionEpoch int32
	MaxWaitMs         int32
	MinBytes          int32
	MaxBytes          int32
	MaxRecords        int32
	BatchSize         int32
	ShareAcquireMode  int8 // v2+: 0=batch-optimized, 1=record-limit
	IsRenewAck        bool // v2+: Renew ack batches present
	Partitions        []ShareFetchPartition
}

// ShareFetchResponse is parsed ShareFetch response data.
type ShareFetchResponse struct {
	ThrottleTimeMs           int32
	ErrorCode                int16
	AcquisitionLockTimeoutMs int32
	Records                  []FetchedRecord
}

// EncodeShareFetchRequest encodes API 78 flex v1+.
func EncodeShareFetchRequest(apiVersion int16, req ShareFetchRequest) []byte {
	buf := wire.NewBuffer(512)
	buf.WriteCompactString(req.GroupID)
	buf.WriteCompactString(req.MemberID)
	buf.WriteInt32(req.ShareSessionEpoch)
	buf.WriteInt32(req.MaxWaitMs)
	buf.WriteInt32(req.MinBytes)
	buf.WriteInt32(req.MaxBytes)
	buf.WriteInt32(req.MaxRecords)
	buf.WriteInt32(req.BatchSize)
	if apiVersion >= 2 {
		buf.WriteInt8(req.ShareAcquireMode)
		buf.WriteBool(req.IsRenewAck)
	}

	byTopic := map[wire.UUID][]ShareFetchPartition{}
	order := make([]wire.UUID, 0)
	for _, p := range req.Partitions {
		if _, ok := byTopic[p.TopicID]; !ok {
			order = append(order, p.TopicID)
		}
		byTopic[p.TopicID] = append(byTopic[p.TopicID], p)
	}
	buf.WriteCompactArrayLen(len(order))
	for _, tid := range order {
		buf.WriteUUID(tid)
		parts := byTopic[tid]
		buf.WriteCompactArrayLen(len(parts))
		for _, p := range parts {
			buf.WriteInt32(p.Partition)
			buf.WriteCompactArrayLen(len(p.AckBatches))
			for _, ab := range p.AckBatches {
				buf.WriteInt64(ab.FirstOffset)
				buf.WriteInt64(ab.LastOffset)
				buf.WriteCompactArrayLen(1)
				buf.WriteInt8(int8(ab.Type))
				buf.WriteEmptyTagSection()
			}
			buf.WriteEmptyTagSection()
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteCompactArrayLen(0) // forgotten_topics_data
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeShareFetchResponse decodes API 78 flex response v1.
func DecodeShareFetchResponse(body []byte, topicName func(wire.UUID) (string, bool)) (ShareFetchResponse, error) {
	buf := wire.FromBytes(body)
	var out ShareFetchResponse
	if _, err := buf.ReadInt32(); err != nil { // throttle
		return out, err
	}
	topErr, err := buf.ReadInt16()
	if err != nil {
		return out, err
	}
	if _, err := buf.ReadCompactNullableString(); err != nil { // error_message
		return out, err
	}
	lockMs, err := buf.ReadInt32()
	if err != nil {
		return out, err
	}
	if topErr != 0 {
		return out, apiError("share fetch", topErr)
	}

	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return out, err
	}
	out.AcquisitionLockTimeoutMs = lockMs
	for i := 1; i < int(nTopics); i++ {
		tid, err := buf.ReadUUID()
		if err != nil {
			return out, err
		}
		topic, ok := topicName(tid)
		if !ok {
			topic = tid.String()
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return out, err
		}
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return out, err
			}
			errCode, err := buf.ReadInt16()
			if err != nil {
				return out, err
			}
			if _, err := buf.ReadCompactNullableString(); err != nil {
				return out, err
			}
			if _, err := buf.ReadInt16(); err != nil { // acknowledge_error_code
				return out, err
			}
			if _, err := buf.ReadCompactNullableString(); err != nil { // acknowledge_error_message
				return out, err
			}
			if _, err := buf.ReadInt32(); err != nil { // current_leader.leader_id
				return out, err
			}
			if _, err := buf.ReadInt32(); err != nil { // current_leader.leader_epoch
				return out, err
			}
			records, err := buf.ReadCompactBytes()
			if err != nil {
				return out, err
			}
			if errCode != 0 {
				return out, fmt.Errorf("protocol: share fetch partition %s-%d error %d", topic, part, errCode)
			}
			if len(records) > 0 {
				recs, err := decodeRecordBatch(topic, part, records)
				if err != nil {
					return out, err
				}
				out.Records = append(out.Records, recs...)
			}
			nAcq, err := buf.ReadUvarint()
			if err != nil {
				return out, err
			}
			for k := 1; k < int(nAcq); k++ {
				if _, err := buf.ReadInt64(); err != nil { // first_offset
					return out, err
				}
				if _, err := buf.ReadInt64(); err != nil { // last_offset
					return out, err
				}
				if _, err := buf.ReadInt16(); err != nil { // delivery_count
					return out, err
				}
			}
			if err := buf.SkipTagSection(); err != nil {
				return out, err
			}
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return out, err
	}
	return out, nil
}
