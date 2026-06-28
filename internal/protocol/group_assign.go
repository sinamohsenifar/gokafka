package protocol

import (
	"sort"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

// MemberSubscription is a consumer group member's topic subscription from JoinGroup metadata.
type MemberSubscription struct {
	MemberID string
	Topics   []string
}

// DecodeConsumerSubscription parses consumer metadata from JoinGroup member bytes.
// The payload is always legacy ConsumerProtocolSubscription (flexibleVersions=none).
func DecodeConsumerSubscription(_ int16, raw []byte) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	buf := wire.FromBytes(raw)
	if _, err := buf.ReadInt16(); err != nil { // subscription version
		return nil, err
	}
	nTopics, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	topics := make([]string, 0, nTopics)
	for i := int32(0); i < nTopics; i++ {
		t, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		topics = append(topics, t)
	}
	return topics, nil
}

// EncodeMemberAssignment encodes partition assignment bytes (range/roundrobin protocol v0).
func EncodeMemberAssignment(assignments []TopicPartitionAssignment) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteInt16(0)
	buf.WriteInt32(int32(len(assignments)))
	for _, a := range assignments {
		buf.WriteString(a.Topic)
		buf.WriteInt32(int32(len(a.Partitions)))
		for _, p := range a.Partitions {
			buf.WriteInt32(p)
		}
	}
	return buf.Bytes()
}

// ComputeGroupAssignments runs the group leader assignor for all members.
func ComputeGroupAssignments(protocolName string, members []MemberSubscription, topicPartitions map[string][]int32) map[string][]byte {
	if len(members) == 0 {
		return nil
	}
	sorted := append([]MemberSubscription(nil), members...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].MemberID < sorted[j].MemberID })

	byMember := make(map[string][]TopicPartitionAssignment, len(sorted))
	memberIDs := make([]string, len(sorted))
	for i, m := range sorted {
		memberIDs[i] = m.MemberID
		byMember[m.MemberID] = nil
	}

	topicMembers := map[string][]string{}
	for _, m := range sorted {
		for _, t := range m.Topics {
			topicMembers[t] = append(topicMembers[t], m.MemberID)
		}
	}

	for topic, mids := range topicMembers {
		sort.Strings(mids)
		parts := topicPartitions[topic]
		if len(parts) == 0 {
			continue
		}
		sortedParts := append([]int32(nil), parts...)
		sort.Slice(sortedParts, func(i, j int) bool { return sortedParts[i] < sortedParts[j] })
		switch protocolName {
		case "roundrobin":
			assignRoundRobin(topic, sortedParts, mids, byMember)
		case "sticky", "cooperative-sticky":
			assignSticky(topic, sortedParts, mids, byMember)
		default:
			assignRange(topic, sortedParts, mids, byMember)
		}
	}

	out := make(map[string][]byte, len(byMember))
	for mid, a := range byMember {
		out[mid] = EncodeMemberAssignment(a)
	}
	return out
}

func assignRange(topic string, parts []int32, members []string, byMember map[string][]TopicPartitionAssignment) {
	n := len(members)
	if n == 0 {
		return
	}
	per := len(parts) / n
	extra := len(parts) % n
	idx := 0
	for i, mid := range members {
		count := per
		if i < extra {
			count++
		}
		if count == 0 {
			continue
		}
		slice := parts[idx : idx+count]
		idx += count
		appendTopicParts(byMember, mid, topic, slice)
	}
}

func assignRoundRobin(topic string, parts []int32, members []string, byMember map[string][]TopicPartitionAssignment) {
	if len(members) == 0 {
		return
	}
	buckets := make([][]int32, len(members))
	for i, p := range parts {
		buckets[i%len(members)] = append(buckets[i%len(members)], p)
	}
	for i, mid := range members {
		if len(buckets[i]) > 0 {
			appendTopicParts(byMember, mid, topic, buckets[i])
		}
	}
}

// assignSticky balances partitions across members (initial sticky assignment without prior state).
func assignSticky(topic string, parts []int32, members []string, byMember map[string][]TopicPartitionAssignment) {
	if len(members) == 0 {
		return
	}
	counts := make(map[string]int, len(members))
	for _, mid := range members {
		counts[mid] = stickyTopicCount(byMember[mid], topic)
	}
	sorted := append([]int32(nil), parts...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for _, p := range sorted {
		mid := stickyPickMember(counts, members)
		counts[mid]++
		appendTopicParts(byMember, mid, topic, []int32{p})
	}
}

func stickyTopicCount(assignments []TopicPartitionAssignment, topic string) int {
	for _, a := range assignments {
		if a.Topic == topic {
			return len(a.Partitions)
		}
	}
	return 0
}

func stickyPickMember(counts map[string]int, members []string) string {
	best := members[0]
	bestCount := counts[best]
	for _, mid := range members[1:] {
		if c := counts[mid]; c < bestCount || (c == bestCount && mid < best) {
			best = mid
			bestCount = c
		}
	}
	return best
}

func appendTopicParts(byMember map[string][]TopicPartitionAssignment, memberID, topic string, parts []int32) {
	assignments := byMember[memberID]
	for i, a := range assignments {
		if a.Topic == topic {
			a.Partitions = append(a.Partitions, parts...)
			assignments[i] = a
			byMember[memberID] = assignments
			return
		}
	}
	byMember[memberID] = append(assignments, TopicPartitionAssignment{Topic: topic, Partitions: parts})
}
