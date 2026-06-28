package protocol

import "testing"

func TestComputeGroupAssignmentsRange(t *testing.T) {
	members := []MemberSubscription{
		{MemberID: "a", Topics: []string{"t"}},
		{MemberID: "b", Topics: []string{"t"}},
	}
	topicParts := map[string][]int32{"t": {0, 1, 2, 3}}
	out := ComputeGroupAssignments("range", members, topicParts)
	if len(out) != 2 {
		t.Fatalf("assignments=%d", len(out))
	}
	a := ParseAssignmentTopics(t, out["a"])
	b := ParseAssignmentTopics(t, out["b"])
	if len(a)+len(b) != 4 {
		t.Fatalf("partitions a=%v b=%v", a, b)
	}
	seen := map[int32]bool{}
	for _, p := range append(a, b...) {
		if seen[p] {
			t.Fatalf("duplicate partition %d", p)
		}
		seen[p] = true
	}
}

func TestComputeGroupAssignmentsSingleMember(t *testing.T) {
	members := []MemberSubscription{{MemberID: "solo", Topics: []string{"t"}}}
	topicParts := map[string][]int32{"t": {0}}
	out := ComputeGroupAssignments("range", members, topicParts)
	if len(out["solo"]) < 4 {
		t.Fatalf("assignment too short: %d bytes", len(out["solo"]))
	}
	parts := ParseAssignmentTopics(t, out["solo"])
	if len(parts) != 1 || parts[0] != 0 {
		t.Fatalf("parts=%v", parts)
	}
}

func TestComputeGroupAssignmentsRoundRobin(t *testing.T) {
	members := []MemberSubscription{
		{MemberID: "m1", Topics: []string{"events"}},
		{MemberID: "m2", Topics: []string{"events"}},
	}
	topicParts := map[string][]int32{"events": {0, 1, 2}}
	out := ComputeGroupAssignments("roundrobin", members, topicParts)
	p1 := ParseAssignmentTopics(t, out["m1"])
	p2 := ParseAssignmentTopics(t, out["m2"])
	if len(p1) != 2 || len(p2) != 1 {
		t.Fatalf("m1=%v m2=%v", p1, p2)
	}
}

func TestComputeGroupAssignmentsSticky(t *testing.T) {
	members := []MemberSubscription{
		{MemberID: "a", Topics: []string{"t"}},
		{MemberID: "b", Topics: []string{"t"}},
		{MemberID: "c", Topics: []string{"t"}},
	}
	topicParts := map[string][]int32{"t": {0, 1, 2, 3, 4}}
	out := ComputeGroupAssignments("sticky", members, topicParts)
	counts := map[string]int{"a": 0, "b": 0, "c": 0}
	for mid, raw := range out {
		parts := ParseAssignmentTopics(t, raw)
		counts[mid] = len(parts)
	}
	if counts["a"] != 2 || counts["b"] != 2 || counts["c"] != 1 {
		t.Fatalf("sticky counts=%v", counts)
	}
}

func TestDecodeConsumerSubscriptionLegacy(t *testing.T) {
	raw := EncodeConsumerSubscription(0, []string{"a", "b"}, false)
	topics, err := DecodeConsumerSubscription(0, raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 2 || topics[0] != "a" || topics[1] != "b" {
		t.Fatalf("topics=%v", topics)
	}
}

func ParseAssignmentTopics(t *testing.T, raw []byte) []int32 {
	t.Helper()
	parsed, err := ParseMemberAssignment(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 1 {
		t.Fatalf("parsed=%+v", parsed)
	}
	return parsed[0].Partitions
}
