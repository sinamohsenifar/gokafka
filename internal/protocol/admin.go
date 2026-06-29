package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// ConsumerGroupListing is a group id from ListGroups.
type ConsumerGroupListing struct {
	GroupID      string
	ProtocolType string
}

func EncodeListGroupsRequest() []byte {
	if VerListGroups >= 4 {
		buf := wire.NewBuffer(8)
		buf.WriteCompactNullableString(nil) // all states
		buf.WriteEmptyTagSection()
		return buf.Bytes()
	}
	return nil
}

func DecodeListGroupsResponse(body []byte) ([]ConsumerGroupListing, error) {
	if VerListGroups >= 3 {
		return decodeListGroupsResponseFlex(body)
	}
	return decodeListGroupsResponseLegacy(body)
}

func decodeListGroupsResponseLegacy(body []byte) ([]ConsumerGroupListing, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	if _, err := buf.ReadInt16(); err != nil { // error_code
		return nil, err
	}
	n, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	out := make([]ConsumerGroupListing, 0, safePrealloc(int(n)))
	for i := 0; i < int(n); i++ {
		id, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		ptype, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		out = append(out, ConsumerGroupListing{GroupID: id, ProtocolType: ptype})
	}
	return out, nil
}

func decodeListGroupsResponseFlex(body []byte) ([]ConsumerGroupListing, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := make([]ConsumerGroupListing, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		id, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		ptype, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		out = append(out, ConsumerGroupListing{GroupID: id, ProtocolType: ptype})
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

// GroupMemberDescription is a member of a consumer group.
type GroupMemberDescription struct {
	MemberID   string
	ClientID   string
	ClientHost string
}

// ConsumerGroupDescription is detailed metadata from DescribeGroups.
type ConsumerGroupDescription struct {
	GroupID      string
	State        string
	ProtocolType string
	Members      []GroupMemberDescription
	ErrorCode    int16
}

func EncodeDescribeGroupsRequest(groups []string) []byte {
	if VerDescribeGroups >= 5 {
		return encodeDescribeGroupsRequestFlex(groups)
	}
	return encodeDescribeGroupsRequestLegacy(groups)
}

func encodeDescribeGroupsRequestLegacy(groups []string) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteInt32(int32(len(groups)))
	for _, g := range groups {
		buf.WriteString(g)
	}
	buf.WriteBool(false)
	return buf.Bytes()
}

func encodeDescribeGroupsRequestFlex(groups []string) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteCompactArrayLen(len(groups))
	for _, g := range groups {
		buf.WriteCompactString(g)
	}
	buf.WriteBool(false)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeDescribeGroupsResponse(body []byte) ([]ConsumerGroupDescription, error) {
	if VerDescribeGroups >= 5 {
		return decodeDescribeGroupsResponseFlex(body)
	}
	return decodeDescribeGroupsResponseLegacy(body)
}

func decodeDescribeGroupsResponseLegacy(body []byte) ([]ConsumerGroupDescription, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	n, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	out := make([]ConsumerGroupDescription, 0, safePrealloc(int(n)))
	for i := int32(0); i < n; i++ {
		errCode, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		groupID, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		state, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		protocolType, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		if _, err := buf.ReadString(); err != nil { // protocol_data
			return nil, err
		}
		nMembers, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		desc := ConsumerGroupDescription{
			GroupID: groupID, State: state, ProtocolType: protocolType, ErrorCode: errCode,
		}
		for j := int32(0); j < nMembers; j++ {
			memberID, err := buf.ReadString()
			if err != nil {
				return nil, err
			}
			if _, err := readNullableString(buf); err != nil { // group_instance_id
				return nil, err
			}
			clientID, err := buf.ReadString()
			if err != nil {
				return nil, err
			}
			clientHost, err := buf.ReadString()
			if err != nil {
				return nil, err
			}
			if _, err := buf.ReadBytes(); err != nil { // member_metadata
				return nil, err
			}
			if _, err := buf.ReadBytes(); err != nil { // member_assignment
				return nil, err
			}
			desc.Members = append(desc.Members, GroupMemberDescription{
				MemberID: memberID, ClientID: clientID, ClientHost: clientHost,
			})
		}
		if _, err := buf.ReadInt32(); err != nil { // authorized_operations
			return nil, err
		}
		out = append(out, desc)
	}
	return out, nil
}

func decodeDescribeGroupsResponseFlex(body []byte) ([]ConsumerGroupDescription, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := make([]ConsumerGroupDescription, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		errCode, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		groupID, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		state, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		protocolType, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		if _, err := buf.ReadCompactString(); err != nil { // protocol_data
			return nil, err
		}
		nMembers, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		desc := ConsumerGroupDescription{
			GroupID: groupID, State: state, ProtocolType: protocolType, ErrorCode: errCode,
		}
		for j := 1; j < int(nMembers); j++ {
			memberID, err := buf.ReadCompactString()
			if err != nil {
				return nil, err
			}
			if _, err := buf.ReadCompactNullableString(); err != nil { // group_instance_id
				return nil, err
			}
			clientID, err := buf.ReadCompactString()
			if err != nil {
				return nil, err
			}
			clientHost, err := buf.ReadCompactString()
			if err != nil {
				return nil, err
			}
			if _, err := buf.ReadCompactBytes(); err != nil { // member_metadata
				return nil, err
			}
			if _, err := buf.ReadCompactBytes(); err != nil { // member_assignment
				return nil, err
			}
			desc.Members = append(desc.Members, GroupMemberDescription{
				MemberID: memberID, ClientID: clientID, ClientHost: clientHost,
			})
			if err := buf.SkipTagSection(); err != nil {
				return nil, err
			}
		}
		if _, err := buf.ReadInt32(); err != nil { // authorized_operations
			return nil, err
		}
		out = append(out, desc)
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

// ConfigEntry is a broker/topic configuration entry.
type ConfigEntry struct {
	Name       string
	Value      string
	IsDefault  bool
	IsReadOnly bool
}

// ConfigResource describes a config resource for DescribeConfigs.
type ConfigResource struct {
	Type int8 // 2=topic, 4=broker
	Name string
}

func EncodeDescribeConfigsRequest(version int16, resources []ConfigResource) []byte {
	if version >= 4 {
		return encodeDescribeConfigsFlex(version, resources)
	}
	return encodeDescribeConfigsLegacy(version, resources)
}

func encodeDescribeConfigsLegacy(version int16, resources []ConfigResource) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteInt32(int32(len(resources)))
	for _, r := range resources {
		buf.WriteInt8(r.Type)
		buf.WriteString(r.Name)
		buf.WriteInt32(-1) // null = all config keys
	}
	buf.WriteBool(false) // include_synonyms
	if version >= 3 {
		buf.WriteBool(false) // include_documentation
	}
	return buf.Bytes()
}

func encodeDescribeConfigsFlex(version int16, resources []ConfigResource) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactArrayLen(len(resources))
	for _, r := range resources {
		buf.WriteInt8(r.Type)
		buf.WriteCompactString(r.Name)
		buf.WriteUvarint(0) // null configuration_keys = all keys
		buf.WriteEmptyTagSection()
	}
	buf.WriteBool(false) // include_synonyms
	if version >= 3 {
		buf.WriteBool(false) // include_documentation
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeDescribeConfigsResponse(version int16, body []byte) (map[string][]ConfigEntry, error) {
	if version >= 4 {
		return decodeDescribeConfigsFlex(version, body)
	}
	return decodeDescribeConfigsLegacy(version, body)
}

func decodeDescribeConfigsLegacy(version int16, body []byte) (map[string][]ConfigEntry, error) {
	buf := wire.FromBytes(body)
	throttle, err := buf.ReadInt32()
	_ = throttle
	if err != nil {
		return nil, err
	}
	n, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	out := map[string][]ConfigEntry{}
	for i := 0; i < int(n); i++ {
		errCode, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		if _, err := readNullableString(buf); err != nil { // error_message
			return nil, err
		}
		rtype, err := buf.ReadInt8()
		if err != nil {
			return nil, err
		}
		_ = rtype
		name, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		nEntries, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		if errCode != 0 {
			for j := 0; j < int(nEntries); j++ {
				if _, err := buf.ReadString(); err != nil {
					return nil, err
				}
				if _, err := readNullableString(buf); err != nil {
					return nil, err
				}
				if _, err := buf.ReadBool(); err != nil {
					return nil, err
				}
				if version < 1 {
					if _, err := buf.ReadBool(); err != nil {
						return nil, err
					}
				}
				if version >= 1 {
					if _, err := buf.ReadInt8(); err != nil {
						return nil, err
					}
				}
				if _, err := buf.ReadBool(); err != nil {
					return nil, err
				}
				if version >= 1 {
					nSyn, err := buf.ReadInt32()
					if err != nil {
						return nil, err
					}
					for k := int32(0); k < nSyn; k++ {
						if _, err := buf.ReadString(); err != nil {
							return nil, err
						}
						if _, err := readNullableString(buf); err != nil {
							return nil, err
						}
						if _, err := buf.ReadInt8(); err != nil {
							return nil, err
						}
					}
				}
			}
			continue
		}
		var entries []ConfigEntry
		for j := 0; j < int(nEntries); j++ {
			ename, err := buf.ReadString()
			if err != nil {
				return nil, err
			}
			val, err := readNullableString(buf)
			if err != nil {
				return nil, err
			}
			readOnly, err := buf.ReadBool()
			if err != nil {
				return nil, err
			}
			isDefault := false
			if version < 1 {
				isDefault, err = buf.ReadBool()
				if err != nil {
					return nil, err
				}
			}
			if version >= 1 {
				if _, err := buf.ReadInt8(); err != nil { // config_source
					return nil, err
				}
			}
			if _, err := buf.ReadBool(); err != nil { // is_sensitive
				return nil, err
			}
			if version >= 1 {
				nSyn, err := buf.ReadInt32()
				if err != nil {
					return nil, err
				}
				for k := int32(0); k < nSyn; k++ {
					if _, err := buf.ReadString(); err != nil {
						return nil, err
					}
					if _, err := readNullableString(buf); err != nil {
						return nil, err
					}
					if _, err := buf.ReadInt8(); err != nil {
						return nil, err
					}
				}
			}
			if version >= 3 {
				if _, err := buf.ReadInt8(); err != nil { // config_type
					return nil, err
				}
				if _, err := readNullableString(buf); err != nil { // config_documentation
					return nil, err
				}
			}
			entries = append(entries, ConfigEntry{Name: ename, Value: val, IsReadOnly: readOnly, IsDefault: isDefault})
		}
		out[name] = entries
	}
	return out, nil
}

func decodeDescribeConfigsFlex(version int16, body []byte) (map[string][]ConfigEntry, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := map[string][]ConfigEntry{}
	for i := 1; i < int(n); i++ {
		errCode, err := buf.ReadInt16()
		if err != nil {
			return nil, err
		}
		if _, err := buf.ReadCompactNullableString(); err != nil { // error_message
			return nil, err
		}
		rtype, err := buf.ReadInt8()
		if err != nil {
			return nil, err
		}
		_ = rtype
		name, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		if errCode != 0 {
			if err := buf.SkipTagSection(); err != nil {
				return nil, err
			}
			continue
		}
		nEntries, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		var entries []ConfigEntry
		for j := 1; j < int(nEntries); j++ {
			ename, err := buf.ReadCompactString()
			if err != nil {
				return nil, err
			}
			val, err := buf.ReadCompactNullableString()
			if err != nil {
				return nil, err
			}
			readOnly, err := buf.ReadBool()
			if err != nil {
				return nil, err
			}
			isDefault := false
			if version < 1 {
				isDefault, err = buf.ReadBool()
				if err != nil {
					return nil, err
				}
			}
			if version >= 1 {
				if _, err := buf.ReadInt8(); err != nil { // config_source
					return nil, err
				}
			}
			if _, err := buf.ReadBool(); err != nil { // is_sensitive
				return nil, err
			}
			if version >= 1 {
				nSyn, err := buf.ReadUvarint()
				if err != nil {
					return nil, err
				}
				for k := 1; k < int(nSyn); k++ {
					if _, err := buf.ReadCompactString(); err != nil {
						return nil, err
					}
					if _, err := buf.ReadCompactNullableString(); err != nil {
						return nil, err
					}
					if _, err := buf.ReadInt8(); err != nil {
						return nil, err
					}
				}
			}
			if version >= 3 {
				if _, err := buf.ReadInt8(); err != nil { // config_type
					return nil, err
				}
				if _, err := buf.ReadCompactNullableString(); err != nil { // config_documentation
					return nil, err
				}
			}
			entries = append(entries, ConfigEntry{Name: ename, Value: val, IsReadOnly: readOnly, IsDefault: isDefault})
			if err := buf.SkipTagSection(); err != nil {
				return nil, err
			}
		}
		out[name] = entries
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

const (
	ConfigResourceTopic  int8 = 2
	ConfigResourceBroker int8 = 4
	ConfigResourceGroup  int8 = 32
)
