package protocol

import (
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// Broker describes a Kafka broker from metadata.
type Broker struct {
	NodeID int32
	Host   string
	Port   int32
	Rack   string
}

// PartitionMeta holds partition leadership info.
type PartitionMeta struct {
	Topic       string
	Partition   int32
	Leader      int32
	LeaderEpoch int32 // -1 when the broker does not report it (metadata < v7)
	Replicas    []int32
	ISR         []int32
	OfflineRepl []int32
	ErrorCode   int16
}

// MetadataResponse is parsed broker/topic metadata.
type MetadataResponse struct {
	Brokers    []Broker
	Controller int32
	ClusterID  string
	Topics     []TopicMeta
}

type TopicMeta struct {
	Name       string
	TopicID    wire.UUID
	Partitions []PartitionMeta
	ErrorCode  int16
}

func EncodeMetadataRequest(version int16, topics []string) []byte {
	if version <= 8 {
		return encodeMetadataRequestLegacy(topics)
	}
	return encodeMetadataRequestFlex(version, topics)
}

func DecodeMetadataResponse(version int16, body []byte) (MetadataResponse, error) {
	if version <= 8 {
		return decodeMetadataResponseLegacy(body)
	}
	return decodeMetadataResponseFlex(version, body)
}

func decodeMetadataResponseFlex(version int16, body []byte) (MetadataResponse, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return MetadataResponse{}, err
	}

	nBrokers, err := buf.ReadUvarint()
	if err != nil {
		return MetadataResponse{}, err
	}
	var resp MetadataResponse
	for i := 1; i < int(nBrokers); i++ {
		nodeID, err := buf.ReadInt32()
		if err != nil {
			return MetadataResponse{}, err
		}
		host, err := buf.ReadCompactString()
		if err != nil {
			return MetadataResponse{}, err
		}
		port, err := buf.ReadInt32()
		if err != nil {
			return MetadataResponse{}, err
		}
		rack, err := buf.ReadCompactNullableString()
		if err != nil {
			return MetadataResponse{}, err
		}
		resp.Brokers = append(resp.Brokers, Broker{NodeID: nodeID, Host: host, Port: port, Rack: rack})
		if err := buf.SkipTagSection(); err != nil {
			return MetadataResponse{}, err
		}
	}

	clusterID, err := buf.ReadCompactNullableString()
	if err != nil {
		return MetadataResponse{}, err
	}
	resp.ClusterID = clusterID

	controller, err := buf.ReadInt32()
	if err != nil {
		return MetadataResponse{}, err
	}
	resp.Controller = controller

	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return MetadataResponse{}, err
	}
	for i := 1; i < int(nTopics); i++ {
		errCode, err := buf.ReadInt16()
		if err != nil {
			return MetadataResponse{}, err
		}
		var name string
		if version >= 12 {
			name, err = buf.ReadCompactNullableString()
		} else {
			name, err = buf.ReadCompactString()
		}
		if err != nil {
			return MetadataResponse{}, err
		}
		var topicID wire.UUID
		if version >= 10 {
			if topicID, err = buf.ReadUUID(); err != nil {
				return MetadataResponse{}, err
			}
		}
		isInternal, err := buf.ReadBool()
		_ = isInternal
		if err != nil {
			return MetadataResponse{}, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return MetadataResponse{}, err
		}
		tm := TopicMeta{Name: name, TopicID: topicID, ErrorCode: errCode}
		for j := 1; j < int(nParts); j++ {
			pErr, err := buf.ReadInt16()
			if err != nil {
				return MetadataResponse{}, err
			}
			partID, err := buf.ReadInt32()
			if err != nil {
				return MetadataResponse{}, err
			}
			leader, err := buf.ReadInt32()
			if err != nil {
				return MetadataResponse{}, err
			}
			leaderEpoch := int32(-1)
			if version >= 7 {
				if leaderEpoch, err = buf.ReadInt32(); err != nil { // leader_epoch
					return MetadataResponse{}, err
				}
			}
			replicas, err := readCompactInt32Array(buf)
			if err != nil {
				return MetadataResponse{}, err
			}
			isr, err := readCompactInt32Array(buf)
			if err != nil {
				return MetadataResponse{}, err
			}
			offline, err := readCompactInt32Array(buf)
			if err != nil {
				return MetadataResponse{}, err
			}
			tm.Partitions = append(tm.Partitions, PartitionMeta{
				Topic: name, Partition: partID, Leader: leader, LeaderEpoch: leaderEpoch,
				Replicas: replicas, ISR: isr, OfflineRepl: offline, ErrorCode: pErr,
			})
			if err := buf.SkipTagSection(); err != nil {
				return MetadataResponse{}, err
			}
		}
		if version >= 8 {
			if _, err := buf.ReadInt32(); err != nil { // topic_authorized_operations
				return MetadataResponse{}, err
			}
		}
		resp.Topics = append(resp.Topics, tm)
		if err := buf.SkipTagSection(); err != nil {
			return MetadataResponse{}, err
		}
	}
	if version >= 8 && version <= 10 {
		if _, err := buf.ReadInt32(); err != nil { // cluster_authorized_operations
			return MetadataResponse{}, err
		}
	}
	if version >= 13 {
		if _, err := buf.ReadInt16(); err != nil { // top-level error_code
			return MetadataResponse{}, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return MetadataResponse{}, err
	}
	return resp, nil
}

func readCompactInt32Array(buf *wire.Buffer) ([]int32, error) {
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := make([]int32, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		v, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func encodeMetadataRequestLegacy(topics []string) []byte {
	buf := wire.NewBuffer(64)
	if topics == nil {
		buf.WriteInt32(-1)
	} else {
		buf.WriteInt32(int32(len(topics)))
		for _, t := range topics {
			buf.WriteString(t)
		}
	}
	buf.WriteBool(true)
	buf.WriteBool(false)
	buf.WriteBool(false)
	return buf.Bytes()
}

func encodeMetadataRequestFlex(version int16, topics []string) []byte {
	buf := wire.NewBuffer(64)
	if topics == nil {
		buf.WriteUvarint(0)
	} else {
		buf.WriteCompactArrayLen(len(topics))
		for _, t := range topics {
			if version >= 10 {
				var zero wire.UUID
				buf.WriteUUID(zero)
				buf.WriteCompactNullableString(&t)
			} else {
				buf.WriteCompactString(t)
			}
			buf.WriteEmptyTagSection()
		}
	}
	buf.WriteBool(true)
	if version <= 10 {
		buf.WriteBool(false)
	}
	buf.WriteBool(false)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func decodeMetadataResponseLegacy(body []byte) (MetadataResponse, error) {
	buf := wire.FromBytes(body)
	throttle, err := buf.ReadInt32()
	_ = throttle
	if err != nil {
		return MetadataResponse{}, err
	}
	nBrokers, err := buf.ReadInt32()
	if err != nil {
		return MetadataResponse{}, err
	}
	var resp MetadataResponse
	for i := 0; i < int(nBrokers); i++ {
		nodeID, err := buf.ReadInt32()
		if err != nil {
			return MetadataResponse{}, err
		}
		host, err := buf.ReadString()
		if err != nil {
			return MetadataResponse{}, err
		}
		port, err := buf.ReadInt32()
		if err != nil {
			return MetadataResponse{}, err
		}
		rack, err := readNullableString(buf)
		if err != nil {
			return MetadataResponse{}, err
		}
		resp.Brokers = append(resp.Brokers, Broker{NodeID: nodeID, Host: host, Port: port, Rack: rack})
	}
	clusterID, err := readNullableString(buf)
	if err != nil {
		return MetadataResponse{}, err
	}
	resp.ClusterID = clusterID
	controller, err := buf.ReadInt32()
	if err != nil {
		return MetadataResponse{}, err
	}
	resp.Controller = controller
	if clusterID != "" {
		resp.ClusterID = clusterID
	}

	nTopics, err := buf.ReadInt32()
	if err != nil {
		return MetadataResponse{}, err
	}
	for i := 0; i < int(nTopics); i++ {
		errCode, err := buf.ReadInt16()
		if err != nil {
			return MetadataResponse{}, err
		}
		name, err := buf.ReadString()
		if err != nil {
			return MetadataResponse{}, err
		}
		isInternal, err := buf.ReadBool()
		_ = isInternal
		if err != nil {
			return MetadataResponse{}, err
		}
		nParts, err := buf.ReadInt32()
		if err != nil {
			return MetadataResponse{}, err
		}
		tm := TopicMeta{Name: name, ErrorCode: errCode}
		for j := 0; j < int(nParts); j++ {
			pErr, err := buf.ReadInt16()
			if err != nil {
				return MetadataResponse{}, err
			}
			partID, err := buf.ReadInt32()
			if err != nil {
				return MetadataResponse{}, err
			}
			leader, err := buf.ReadInt32()
			if err != nil {
				return MetadataResponse{}, err
			}
			leaderEpoch, err := buf.ReadInt32() // leader_epoch
			if err != nil {
				return MetadataResponse{}, err
			}
			replicas, err := readInt32Array(buf)
			if err != nil {
				return MetadataResponse{}, err
			}
			isr, err := readInt32Array(buf)
			if err != nil {
				return MetadataResponse{}, err
			}
			offline, err := readInt32Array(buf)
			if err != nil {
				return MetadataResponse{}, err
			}
			tm.Partitions = append(tm.Partitions, PartitionMeta{
				Topic: name, Partition: partID, Leader: leader, LeaderEpoch: leaderEpoch,
				Replicas: replicas, ISR: isr, OfflineRepl: offline, ErrorCode: pErr,
			})
		}
		_, _ = buf.ReadInt32() // topic_authorized_operations
		resp.Topics = append(resp.Topics, tm)
	}
	_, _ = buf.ReadInt32() // cluster_authorized_operations
	return resp, nil
}

func readNullableString(buf *wire.Buffer) (string, error) {
	n, err := buf.ReadInt16()
	if err != nil {
		return "", err
	}
	if n < 0 {
		return "", nil
	}
	if buf.I+int(n) > len(buf.B) {
		return "", wire.ErrShortBuffer
	}
	s := string(buf.B[buf.I : buf.I+int(n)])
	buf.I += int(n)
	return s, nil
}

func readInt32Array(buf *wire.Buffer) ([]int32, error) {
	n, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	out := make([]int32, 0, safePrealloc(int(n)))
	for i := 0; i < int(n); i++ {
		v, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
