package protocol

import (
	"errors"
	"fmt"
)

var ErrUnknownTopic = errors.New("protocol: unknown topic or partition")

// ErrRebalanceInProgress indicates the consumer group is rebalancing.
var ErrRebalanceInProgress = errors.New("protocol: rebalance in progress")

// ErrLeaderEpochChanged indicates a fetch hit a stale/changed partition leader
// (NOT_LEADER_OR_FOLLOWER, FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH); the caller
// should refresh metadata and retry. KIP-320.
var ErrLeaderEpochChanged = errors.New("protocol: partition leader or epoch changed")

// ErrUnknownTopicID indicates a Fetch v13+ request referenced a topic id the
// broker no longer knows (UNKNOWN_TOPIC_ID, code 100) — e.g. the topic was
// deleted/recreated; the caller should refresh metadata and retry. KIP-516.
var ErrUnknownTopicID = errors.New("protocol: unknown topic id")

// ErrMemberIDRequired is returned when the broker assigns a member id (KIP-394); retry JoinGroup with that id.
var ErrMemberIDRequired = errors.New("protocol: member id required")

// ErrorCodeMemberIDRequired is Kafka error code 79 (MEMBER_ID_REQUIRED).
const ErrorCodeMemberIDRequired int16 = 79

const (
	ErrorCodeCoordinatorLoadInProgress int16 = 14
	ErrorCodeCoordinatorNotAvailable   int16 = 15
	ErrorCodeNotCoordinator            int16 = 16
)

// CoordinatorRetriable reports whether a coordinator lookup or coordinator RPC should be retried.
func CoordinatorRetriable(code int16) bool {
	return code == ErrorCodeCoordinatorLoadInProgress ||
		code == ErrorCodeCoordinatorNotAvailable ||
		code == ErrorCodeNotCoordinator
}

// APIErrorCode returns the Kafka error code when err is a protocol APIError.
func APIErrorCode(err error) (int16, bool) {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Code, true
	}
	return 0, false
}

// APIError is a non-zero Kafka error code from a protocol response.
type APIError struct {
	Op   string
	Code int16
}

func (e *APIError) Error() string {
	return fmt.Sprintf("protocol: %s error %d", e.Op, e.Code)
}

func apiError(op string, code int16) error {
	return &APIError{Op: op, Code: code}
}
