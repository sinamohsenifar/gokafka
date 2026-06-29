package protocol

import (
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

const (
	APIDescribeAcls int16 = 29
	APICreateAcls   int16 = 30
	APIDeleteAcls   int16 = 31

	VerCreateAcls   int16 = 1
	VerDescribeAcls int16 = 1
	VerDeleteAcls   int16 = 1
)

// ResourceType for ACL bindings.
type ResourceType int8

const (
	ResourceTopic           ResourceType = 2
	ResourceGroup           ResourceType = 3
	ResourceCluster         ResourceType = 4
	ResourceTransactionalID ResourceType = 5
)

// ACLOperation mirrors Kafka ACL operations.
type ACLOperation int8

const (
	ACLOpAny      ACLOperation = 1
	ACLOpRead     ACLOperation = 3
	ACLOpWrite    ACLOperation = 4
	ACLOpCreate   ACLOperation = 5
	ACLOpDelete   ACLOperation = 6
	ACLOpAlter    ACLOperation = 7
	ACLOpDescribe ACLOperation = 8
)

// ACLPermissionType mirrors Kafka ACL permission types.
type ACLPermissionType int8

const (
	ACLPermAny   ACLPermissionType = 1
	ACLPermDeny  ACLPermissionType = 2
	ACLPermAllow ACLPermissionType = 3
)

// ACLBinding describes a single ACL rule.
type ACLBinding struct {
	ResourceType ResourceType
	ResourceName string
	Principal    string
	Host         string
	Operation    ACLOperation
	Permission   ACLPermissionType
}

func EncodeCreateACLsRequest(version int16, bindings []ACLBinding) []byte {
	switch {
	case version >= 2:
		return encodeCreateACLsFlex(bindings)
	case version == 1:
		return encodeCreateACLsV1(bindings)
	default:
		return encodeCreateACLsV0(bindings)
	}
}

func encodeCreateACLsV0(bindings []ACLBinding) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteInt32(int32(len(bindings)))
	for _, b := range bindings {
		buf.WriteInt8(int8(b.ResourceType))
		buf.WriteString(b.ResourceName)
		buf.WriteString(b.Principal)
		buf.WriteString(b.Host)
		buf.WriteInt8(int8(b.Operation))
		buf.WriteInt8(int8(b.Permission))
	}
	return buf.Bytes()
}

func encodeCreateACLsV1(bindings []ACLBinding) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteInt32(int32(len(bindings)))
	for _, b := range bindings {
		buf.WriteInt8(int8(b.ResourceType))
		buf.WriteString(b.ResourceName)
		buf.WriteInt8(3) // literal pattern
		buf.WriteString(b.Principal)
		buf.WriteString(b.Host)
		buf.WriteInt8(int8(b.Operation))
		buf.WriteInt8(int8(b.Permission))
	}
	return buf.Bytes()
}

func encodeCreateACLsFlex(bindings []ACLBinding) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactArrayLen(len(bindings))
	for _, b := range bindings {
		buf.WriteInt8(int8(b.ResourceType))
		buf.WriteCompactString(b.ResourceName)
		buf.WriteInt8(3) // literal pattern
		buf.WriteCompactString(b.Principal)
		buf.WriteCompactString(b.Host)
		buf.WriteInt8(int8(b.Operation))
		buf.WriteInt8(int8(b.Permission))
		buf.WriteEmptyTagSection()
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func EncodeDescribeACLsFilter(version int16, resourceType ResourceType, name string, principal string) []byte {
	switch {
	case version >= 2:
		return encodeDescribeACLsFilterFlex(resourceType, name, principal)
	case version == 1:
		return encodeDescribeACLsFilterV1(resourceType, name, principal)
	default:
		return encodeDescribeACLsFilterV0(resourceType, name, principal)
	}
}

func encodeDescribeACLsFilterV0(resourceType ResourceType, name string, principal string) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteInt8(int8(resourceType))
	buf.WriteNullableString(&name)
	buf.WriteNullableString(&principal)
	host := "*"
	buf.WriteNullableString(&host)
	buf.WriteInt8(int8(ACLOpAny))
	buf.WriteInt8(int8(ACLPermAny))
	return buf.Bytes()
}

func encodeDescribeACLsFilterV1(resourceType ResourceType, name string, principal string) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteInt8(int8(resourceType))
	var nameFilter *string
	if name != "" {
		nameFilter = &name
	}
	buf.WriteNullableString(nameFilter)
	buf.WriteInt8(3) // literal pattern
	var principalFilter *string
	if principal != "" {
		principalFilter = &principal
	}
	buf.WriteNullableString(principalFilter)
	buf.WriteNullableString(nil) // any host
	buf.WriteInt8(int8(ACLOpAny))
	buf.WriteInt8(int8(ACLPermAny))
	return buf.Bytes()
}

func encodeDescribeACLsFilterFlex(resourceType ResourceType, name string, principal string) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteInt8(int8(resourceType))
	buf.WriteCompactNullableString(&name)
	buf.WriteInt8(3)
	buf.WriteCompactNullableString(&principal)
	buf.WriteCompactNullableString(nil)
	buf.WriteInt8(int8(ACLOpAny))
	buf.WriteInt8(int8(ACLPermAny))
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeCreateACLsResponse(version int16, body []byte) ([]TopicMutationResult, error) {
	if version >= 2 {
		return decodeCreateACLsResponseFlex(body)
	}
	return decodeCreateACLsResponseLegacy(body)
}

func decodeCreateACLsResponseLegacy(body []byte) ([]TopicMutationResult, error) {
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
		code, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		if _, err := readNullableString(buf); err != nil {
			return nil, err
		}
		out = append(out, TopicMutationResult{ErrorCode: code})
	}
	return out, nil
}

func decodeCreateACLsResponseFlex(body []byte) ([]TopicMutationResult, error) {
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
		code, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		if _, err := buf.ReadCompactNullableString(); err != nil { // error_message
			return nil, err
		}
		if _, err := buf.ReadInt8(); err != nil { // resource_type
			return nil, err
		}
		if _, err := buf.ReadCompactNullableString(); err != nil { // resource_name
			return nil, err
		}
		if _, err := buf.ReadInt8(); err != nil { // pattern_type
			return nil, err
		}
		if _, err := buf.ReadCompactNullableString(); err != nil { // principal
			return nil, err
		}
		if _, err := buf.ReadCompactNullableString(); err != nil { // host
			return nil, err
		}
		if _, err := buf.ReadInt8(); err != nil { // operation
			return nil, err
		}
		if _, err := buf.ReadInt8(); err != nil { // permission_type
			return nil, err
		}
		out = append(out, TopicMutationResult{ErrorCode: code})
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

func DecodeDescribeACLsResponse(version int16, body []byte) ([]ACLBinding, error) {
	if version >= 2 {
		return decodeDescribeACLsResponseFlex(version, body)
	}
	return decodeDescribeACLsResponseLegacy(version, body)
}

func decodeDescribeACLsResponseLegacy(version int16, body []byte) ([]ACLBinding, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	errCode, err := buf.ReadInt16()
	if err != nil {
		return nil, err
	}
	if _, err := readNullableString(buf); err != nil { // top-level error_message
		return nil, err
	}
	if errCode != 0 {
		return nil, fmt.Errorf("protocol: describe acls error %d", errCode)
	}
	n, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	var out []ACLBinding
	for i := 0; i < int(n); i++ {
		rt, err := buf.ReadInt8()
		if err != nil {
			return nil, err
		}
		name, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		if version >= 1 {
			if _, err := buf.ReadInt8(); err != nil { // pattern_type
				return nil, err
			}
		}
		nACLs, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		for j := 0; j < int(nACLs); j++ {
			principal, err := buf.ReadString()
			if err != nil {
				return nil, err
			}
			host, err := buf.ReadString()
			if err != nil {
				return nil, err
			}
			op, err := buf.ReadInt8()
			if err != nil {
				return nil, err
			}
			perm, err := buf.ReadInt8()
			if err != nil {
				return nil, err
			}
			out = append(out, ACLBinding{
				ResourceType: ResourceType(rt), ResourceName: name,
				Principal: principal, Host: host,
				Operation: ACLOperation(op), Permission: ACLPermissionType(perm),
			})
		}
	}
	return out, nil
}

func decodeDescribeACLsResponseFlex(version int16, body []byte) ([]ACLBinding, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	errCode, err := buf.ReadInt16()
	if err != nil {
		return nil, err
	}
	if _, err := buf.ReadCompactNullableString(); err != nil { // error_message
		return nil, err
	}
	if errCode != 0 {
		return nil, fmt.Errorf("protocol: describe acls error %d", errCode)
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	var out []ACLBinding
	for i := 1; i < int(n); i++ {
		rt, err := buf.ReadInt8()
		if err != nil {
			return nil, err
		}
		name, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		if version >= 1 {
			if _, err := buf.ReadInt8(); err != nil { // pattern_type
				return nil, err
			}
		}
		nACLs, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		for j := 1; j < int(nACLs); j++ {
			principal, err := buf.ReadCompactString()
			if err != nil {
				return nil, err
			}
			host, err := buf.ReadCompactString()
			if err != nil {
				return nil, err
			}
			op, err := buf.ReadInt8()
			if err != nil {
				return nil, err
			}
			perm, err := buf.ReadInt8()
			if err != nil {
				return nil, err
			}
			out = append(out, ACLBinding{
				ResourceType: ResourceType(rt), ResourceName: name,
				Principal: principal, Host: host,
				Operation: ACLOperation(op), Permission: ACLPermissionType(perm),
			})
			if err := buf.SkipTagSection(); err != nil {
				return nil, err
			}
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

func EncodeDeleteACLsFilter(version int16, resourceType ResourceType, name, principal string) []byte {
	return EncodeDescribeACLsFilter(version, resourceType, name, principal)
}

func DecodeDeleteACLsResponse(body []byte) (int, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return 0, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return 0, err
	}
	return int(n) - 1, nil
}
