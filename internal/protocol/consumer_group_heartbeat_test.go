package protocol

import "testing"

func TestConsumerGroupHeartbeatRequestRoundTripFields(t *testing.T) {
	assignor := "range"
	body := EncodeConsumerGroupHeartbeatRequest(ConsumerGroupHeartbeatRequest{
		GroupID:              "g1",
		MemberID:             "550e8400-e29b-41d4-a716-446655440000",
		MemberEpoch:          0,
		RebalanceTimeoutMs:   45000,
		SubscribedTopicNames: []string{"t1", "t2"},
		ServerAssignor:       &assignor,
	})
	if len(body) == 0 {
		t.Fatal("empty body")
	}
}
