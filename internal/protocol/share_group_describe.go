package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// ShareGroupMemberDescription is a member in ShareGroupDescribe.
type ShareGroupMemberDescription struct {
	MemberID             string
	RackID               string
	MemberEpoch          int32
	ClientID             string
	ClientHost           string
	SubscribedTopicNames []string
}

// ShareGroupDescription is detailed share group metadata (API 77 v1).
type ShareGroupDescription struct {
	ErrorCode         int16
	ErrorMessage      string
	GroupID           string
	GroupState        string
	GroupEpoch        int32
	AssignmentEpoch   int32
	AssignorName      string
	Members           []ShareGroupMemberDescription
	AuthorizedOps     int32
}

// EncodeShareGroupDescribeRequest encodes API 77 flex v1.
func EncodeShareGroupDescribeRequest(groups []string, includeAuthorizedOps bool) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactArrayLen(len(groups))
	for _, g := range groups {
		buf.WriteCompactString(g)
	}
	buf.WriteBool(includeAuthorizedOps)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeShareGroupDescribeResponse decodes API 77 flex response v1.
func DecodeShareGroupDescribeResponse(body []byte) ([]ShareGroupDescription, error) {
	buf := wire.FromBytes(body)
	var out []ShareGroupDescription
	if _, err := buf.ReadInt32(); err != nil { // throttle
		return nil, err
	}
	nGroups, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	for i := 1; i < int(nGroups); i++ {
		var g ShareGroupDescription
		if g.ErrorCode, err = buf.ReadInt16(); err != nil {
			return nil, err
		}
		if g.ErrorMessage, err = buf.ReadCompactNullableString(); err != nil {
			return nil, err
		}
		if g.GroupID, err = buf.ReadCompactString(); err != nil {
			return nil, err
		}
		if g.GroupState, err = buf.ReadCompactString(); err != nil {
			return nil, err
		}
		if g.GroupEpoch, err = buf.ReadInt32(); err != nil {
			return nil, err
		}
		if g.AssignmentEpoch, err = buf.ReadInt32(); err != nil {
			return nil, err
		}
		if g.AssignorName, err = buf.ReadCompactString(); err != nil {
			return nil, err
		}
		nMembers, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		for j := 1; j < int(nMembers); j++ {
			var m ShareGroupMemberDescription
			if m.MemberID, err = buf.ReadCompactString(); err != nil {
				return nil, err
			}
			if m.RackID, err = buf.ReadCompactNullableString(); err != nil {
				return nil, err
			}
			if m.MemberEpoch, err = buf.ReadInt32(); err != nil {
				return nil, err
			}
			if m.ClientID, err = buf.ReadCompactString(); err != nil {
				return nil, err
			}
			if m.ClientHost, err = buf.ReadCompactString(); err != nil {
				return nil, err
			}
			nTopics, err := buf.ReadUvarint()
			if err != nil {
				return nil, err
			}
			for k := 1; k < int(nTopics); k++ {
				topic, err := buf.ReadCompactString()
				if err != nil {
					return nil, err
				}
				m.SubscribedTopicNames = append(m.SubscribedTopicNames, topic)
			}
			if err := skipShareMemberAssignment(buf); err != nil {
				return nil, err
			}
			if err := buf.SkipTagSection(); err != nil {
				return nil, err
			}
			g.Members = append(g.Members, m)
		}
		if g.AuthorizedOps, err = buf.ReadInt32(); err != nil {
			return nil, err
		}
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

func skipShareMemberAssignment(buf *wire.Buffer) error {
	assignMark, err := buf.ReadUvarint()
	if err != nil {
		return err
	}
	if assignMark <= 1 {
		return nil
	}
	nTP, err := buf.ReadUvarint()
	if err != nil {
		return err
	}
	for i := 1; i < int(nTP); i++ {
		if _, err := buf.ReadUUID(); err != nil { // topic_id
			return err
		}
		if _, err := buf.ReadCompactString(); err != nil { // topic_name
			return err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return err
		}
		for j := 1; j < int(nParts); j++ {
			if _, err := buf.ReadInt32(); err != nil {
				return err
			}
		}
	}
	return nil
}
