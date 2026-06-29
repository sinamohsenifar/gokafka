package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// TopicMutationResult is the per-topic result of create/delete/partition admin calls.
type TopicMutationResult struct {
	Topic     string
	ErrorCode int16
}

func DecodeCreateTopicsResponse(body []byte) ([]TopicMutationResult, error) {
	if VerCreateTopics >= 5 {
		return decodeCreateTopicsResponseFlex(body)
	}
	return decodeCreateTopicsResponseLegacy(body)
}

func decodeCreateTopicsResponseLegacy(body []byte) ([]TopicMutationResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	n, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	out := make([]TopicMutationResult, 0, safePrealloc(int(n)))
	for i := 0; i < int(n); i++ {
		name, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		code, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		if _, err := readNullableString(buf); err != nil {
			return nil, err
		}
		out = append(out, TopicMutationResult{Topic: name, ErrorCode: code})
	}
	return out, nil
}

func decodeCreateTopicsResponseFlex(body []byte) ([]TopicMutationResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := make([]TopicMutationResult, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		name, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		code, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		if _, err := buf.ReadCompactNullableString(); err != nil { // error_message
			return nil, err
		}
		if _, err := buf.ReadInt32(); err != nil { // num_partitions
			return nil, err
		}
		if _, err := buf.ReadInt16(); err != nil { // replication_factor
			return nil, err
		}
		if _, err := buf.ReadCompactBytes(); err != nil { // configs
			return nil, err
		}
		out = append(out, TopicMutationResult{Topic: name, ErrorCode: code})
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

func DecodeDeleteTopicsResponse(body []byte) ([]TopicMutationResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := make([]TopicMutationResult, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		name, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		code, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		out = append(out, TopicMutationResult{Topic: name, ErrorCode: code})
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

// ConfigAlteration is a single config key/value change.
type ConfigAlteration struct {
	Name  string
	Value *string // nil deletes the config
}

func EncodeAlterConfigsRequest(version int16, resources map[string][]ConfigAlteration) []byte {
	if version >= 2 {
		return encodeAlterConfigsRequestFlex(resources)
	}
	return encodeAlterConfigsRequestLegacy(resources)
}

func encodeAlterConfigsRequestLegacy(resources map[string][]ConfigAlteration) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteInt32(int32(len(resources)))
	for name, alters := range resources {
		buf.WriteInt8(ConfigResourceTopic)
		buf.WriteString(name)
		buf.WriteInt32(int32(len(alters)))
		for _, a := range alters {
			buf.WriteString(a.Name)
			buf.WriteNullableString(a.Value)
		}
	}
	buf.WriteBool(false) // validate_only
	return buf.Bytes()
}

func encodeAlterConfigsRequestFlex(resources map[string][]ConfigAlteration) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactArrayLen(len(resources))
	for name, alters := range resources {
		buf.WriteInt8(ConfigResourceTopic)
		buf.WriteCompactString(name)
		buf.WriteCompactArrayLen(len(alters))
		for _, a := range alters {
			buf.WriteCompactString(a.Name)
			buf.WriteCompactNullableString(a.Value)
			buf.WriteEmptyTagSection()
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteBool(false) // validate_only
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeAlterConfigsResponse(version int16, body []byte) (int16, error) {
	if version >= 2 {
		return decodeAlterConfigsResponseFlex(body)
	}
	return decodeAlterConfigsResponseLegacy(body)
}

func decodeAlterConfigsResponseLegacy(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	n, err := buf.ReadInt32()
	if err != nil {
		return 0, err
	}
	for i := int32(0); i < n; i++ {
		code, err := buf.ReadInt16()
		if err != nil {
			return 0, err
		}
		if _, err := readNullableString(buf); err != nil { // error_message
			return 0, err
		}
		if _, err := buf.ReadInt8(); err != nil { // resource_type
			return 0, err
		}
		if _, err := buf.ReadString(); err != nil { // resource_name
			return 0, err
		}
		if code != 0 {
			return code, nil
		}
	}
	return 0, nil
}

func decodeAlterConfigsResponseFlex(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return 0, err
	}
	for i := 1; i < int(n); i++ {
		code, err := buf.ReadInt16()
		if err != nil {
			return 0, err
		}
		if _, err := buf.ReadCompactNullableString(); err != nil { // error_message
			return 0, err
		}
		if _, err := buf.ReadInt8(); err != nil { // resource_type
			return 0, err
		}
		if _, err := buf.ReadCompactString(); err != nil { // resource_name
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
	return 0, nil
}

// CreatePartitionsSpec adds partitions to a topic.
type CreatePartitionsSpec struct {
	Topic      string
	Count      int32
	Assignment [][]int32 // optional replica assignments per new partition
}

func EncodeCreatePartitionsRequest(version int16, specs []CreatePartitionsSpec, timeoutMs int32) []byte {
	if timeoutMs <= 0 {
		timeoutMs = 30000
	}
	if version >= 2 {
		return encodeCreatePartitionsFlex(specs, timeoutMs)
	}
	return encodeCreatePartitionsLegacy(specs, timeoutMs)
}

func encodeCreatePartitionsLegacy(specs []CreatePartitionsSpec, timeoutMs int32) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteInt32(int32(len(specs)))
	for _, s := range specs {
		buf.WriteString(s.Topic)
		buf.WriteInt32(s.Count)
		buf.WriteInt32(-1) // null replica assignments
	}
	buf.WriteInt32(timeoutMs)
	buf.WriteBool(false)
	return buf.Bytes()
}

func encodeCreatePartitionsFlex(specs []CreatePartitionsSpec, timeoutMs int32) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactArrayLen(len(specs))
	for _, s := range specs {
		buf.WriteCompactString(s.Topic)
		buf.WriteInt32(s.Count)
		if len(s.Assignment) > 0 {
			buf.WriteCompactArrayLen(len(s.Assignment))
			for _, replicas := range s.Assignment {
				buf.WriteCompactArrayLen(len(replicas))
				for _, r := range replicas {
					buf.WriteInt32(r)
				}
			}
		} else {
			buf.WriteUvarint(0) // null broker assignments
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteInt32(timeoutMs)
	buf.WriteBool(false) // validate_only
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeCreatePartitionsResponse(version int16, body []byte) ([]TopicMutationResult, error) {
	if version >= 2 {
		return decodeCreatePartitionsResponseFlex(body)
	}
	return decodeCreatePartitionsResponseLegacy(body)
}

func decodeCreatePartitionsResponseLegacy(body []byte) ([]TopicMutationResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	n, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	out := make([]TopicMutationResult, 0, safePrealloc(int(n)))
	for i := 0; i < int(n); i++ {
		name, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		code, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		if _, err := readNullableString(buf); err != nil { // error_message
			return nil, err
		}
		out = append(out, TopicMutationResult{Topic: name, ErrorCode: code})
	}
	return out, nil
}

func decodeCreatePartitionsResponseFlex(body []byte) ([]TopicMutationResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := make([]TopicMutationResult, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		name, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		code, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		out = append(out, TopicMutationResult{Topic: name, ErrorCode: code})
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

func EncodeOffsetDeleteRequest(version int16, groupID string, offsets map[string][]int32) []byte {
	if version >= 1 {
		return encodeOffsetDeleteFlex(groupID, offsets)
	}
	return encodeOffsetDeleteLegacy(groupID, offsets)
}

func encodeOffsetDeleteLegacy(groupID string, offsets map[string][]int32) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteString(groupID)
	buf.WriteInt32(int32(len(offsets)))
	for topic, parts := range offsets {
		buf.WriteString(topic)
		buf.WriteInt32(int32(len(parts)))
		for _, p := range parts {
			buf.WriteInt32(p)
		}
	}
	return buf.Bytes()
}

func encodeOffsetDeleteFlex(groupID string, offsets map[string][]int32) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactString(groupID)
	buf.WriteCompactArrayLen(len(offsets))
	for topic, parts := range offsets {
		buf.WriteCompactString(topic)
		buf.WriteCompactArrayLen(len(parts))
		for _, p := range parts {
			buf.WriteInt32(p)
		}
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeOffsetDeleteResponse(version int16, body []byte) (int16, error) {
	if version >= 1 {
		return decodeOffsetDeleteResponseFlex(body)
	}
	return decodeOffsetDeleteResponseLegacy(body)
}

func decodeOffsetDeleteResponseLegacy(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	code, err := buf.ReadInt16()
	if err != nil {
		return 0, err
	}
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return 0, err
	}
	if code != 0 {
		return code, nil
	}
	n, err := buf.ReadInt32()
	if err != nil {
		return 0, err
	}
	for i := 0; i < int(n); i++ {
		if _, err := buf.ReadString(); err != nil { // topic
			return 0, err
		}
		nParts, err := buf.ReadInt32()
		if err != nil {
			return 0, err
		}
		for j := 0; j < int(nParts); j++ {
			if _, err := buf.ReadInt32(); err != nil { // partition
				return 0, err
			}
			partCode, err := buf.ReadInt16()
			if err != nil {
				return 0, err
			}
			if partCode != 0 {
				return partCode, nil
			}
		}
	}
	return 0, nil
}

func decodeOffsetDeleteResponseFlex(body []byte) (int16, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return 0, err
	}
	for i := 1; i < int(n); i++ {
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
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return 0, err
	}
	return 0, nil
}

func FirstTopicError(results []TopicMutationResult) (TopicMutationResult, bool) {
	for _, r := range results {
		if r.ErrorCode != 0 {
			return r, true
		}
	}
	return TopicMutationResult{}, false
}

const (
	ConfigOpSet    int8 = 0
	ConfigOpDelete int8 = 1
)

func EncodeIncrementalAlterConfigsRequest(version int16, resourceType int8, resources map[string][]ConfigAlteration) []byte {
	if version >= 1 {
		return encodeIncrementalAlterConfigsFlex(resourceType, resources)
	}
	return encodeIncrementalAlterConfigsLegacy(resourceType, resources)
}

func encodeIncrementalAlterConfigsLegacy(resourceType int8, resources map[string][]ConfigAlteration) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteInt32(int32(len(resources)))
	for name, alters := range resources {
		buf.WriteInt8(resourceType)
		buf.WriteString(name)
		buf.WriteInt32(int32(len(alters)))
		for _, a := range alters {
			buf.WriteString(a.Name)
			buf.WriteInt8(configOp(a))
			if a.Value != nil {
				buf.WriteString(*a.Value)
			} else {
				buf.WriteInt16(-1)
			}
		}
	}
	buf.WriteBool(false)
	return buf.Bytes()
}

func encodeIncrementalAlterConfigsFlex(resourceType int8, resources map[string][]ConfigAlteration) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactArrayLen(len(resources))
	for name, alters := range resources {
		buf.WriteInt8(resourceType)
		buf.WriteCompactString(name)
		buf.WriteCompactArrayLen(len(alters))
		for _, a := range alters {
			buf.WriteCompactString(a.Name)
			buf.WriteInt8(configOp(a))
			buf.WriteCompactNullableString(a.Value)
			buf.WriteEmptyTagSection()
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteBool(false)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func configOp(a ConfigAlteration) int8 {
	if a.Value == nil {
		return ConfigOpDelete
	}
	return ConfigOpSet
}

func DecodeIncrementalAlterConfigsResponse(version int16, body []byte) (int16, error) {
	if version >= 1 {
		return decodeAlterConfigsResponseFlex(body)
	}
	return decodeAlterConfigsResponseLegacy(body)
}

func EncodeDeleteGroupsRequest(groups []string) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteCompactArrayLen(len(groups))
	for _, g := range groups {
		buf.WriteCompactString(g)
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

type GroupMutationResult struct {
	GroupID   string
	ErrorCode int16
}

func DecodeDeleteGroupsResponse(body []byte) ([]GroupMutationResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := make([]GroupMutationResult, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		id, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		code, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		out = append(out, GroupMutationResult{GroupID: id, ErrorCode: code})
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

func FirstGroupError(results []GroupMutationResult) (GroupMutationResult, bool) {
	for _, r := range results {
		if r.ErrorCode != 0 {
			return r, true
		}
	}
	return GroupMutationResult{}, false
}
