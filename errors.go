package gokafka

import (
	"errors"
	"fmt"
	"io"
	"net"
)

var (
	ErrNoBrokers             = errors.New("gokafka: at least one broker is required")
	ErrClosed                = errors.New("gokafka: client is closed")
	ErrNoConsumerGroup       = errors.New("gokafka: consumer group is required")
	ErrNoShareGroup          = errors.New("gokafka: share group is required")
	ErrNoSchemaURL           = errors.New("gokafka: schema registry URL is required")
	ErrTopicNotFound         = errors.New("gokafka: topic not found")
	ErrNoTransactionalID     = errors.New("gokafka: transactional id is required when transactions are enabled")
	ErrTransactionAborted    = errors.New("gokafka: transaction aborted")
	ErrRetriable             = errors.New("gokafka: retriable error")
	ErrInvalidProducerConfig = errors.New("gokafka: idempotent producer requires acks=all")
)

// ErrorCode mirrors Kafka protocol error codes.
type ErrorCode int16

const (
	ErrCodeNone                         ErrorCode = 0
	ErrCodeUnknownTopic                 ErrorCode = 3
	ErrCodeLeaderNotAvail               ErrorCode = 5
	ErrCodeNotLeaderForPart             ErrorCode = 6
	ErrCodeRequestTimedOut              ErrorCode = 7
	ErrCodeNetworkException             ErrorCode = 8
	ErrCodeCoordinatorLoad              ErrorCode = 14
	ErrCodeCoordinatorNotAvailable      ErrorCode = 15
	ErrCodeNotCoordinator               ErrorCode = 16
	ErrCodeNotEnoughReplicas            ErrorCode = 19
	ErrCodeNotEnoughReplicasAfterAppend ErrorCode = 20
	ErrCodeRebalanceInProg              ErrorCode = 27
	ErrCodeOutOfOrderSequence           ErrorCode = 45
	ErrCodeInvalidProducerEpoch         ErrorCode = 47
	ErrCodeInvalidTxnState              ErrorCode = 48
	ErrCodeUnsupportedVersion           ErrorCode = 35
	ErrCodeConcurrentTransactions       ErrorCode = 51
	ErrCodeTransactionAbortable         ErrorCode = 120
	ErrCodeShareSessionNotFound         ErrorCode = 122
	ErrCodeInvalidShareSessionEpoch     ErrorCode = 123
	ErrCodeUnstableOffsetCommit         ErrorCode = 88
)

// KafkaError wraps a broker error code with context.
type KafkaError struct {
	Code      ErrorCode
	Topic     string
	Partition int32
	Msg       string
}

func (e *KafkaError) Error() string {
	if e.Topic != "" {
		return fmt.Sprintf("gokafka: %s (code=%d topic=%s partition=%d)", e.Msg, e.Code, e.Topic, e.Partition)
	}
	return fmt.Sprintf("gokafka: %s (code=%d)", e.Msg, e.Code)
}

func (e *KafkaError) Retriable() bool {
	switch e.Code {
	case ErrCodeLeaderNotAvail, ErrCodeNotLeaderForPart, ErrCodeRequestTimedOut,
		ErrCodeNetworkException, ErrCodeCoordinatorLoad, ErrCodeCoordinatorNotAvailable, ErrCodeNotCoordinator,
		ErrCodeNotEnoughReplicas, ErrCodeNotEnoughReplicasAfterAppend, ErrCodeRebalanceInProg,
		ErrCodeConcurrentTransactions,
		ErrCodeShareSessionNotFound, ErrCodeInvalidShareSessionEpoch:
		return true
	default:
		// OUT_OF_ORDER_SEQUENCE (45) and INVALID_PRODUCER_EPOCH (47) are NOT
		// retriable: on an idempotent producer they are fatal (the local
		// sequence has diverged from the broker; a blind resend under a new
		// producer id would duplicate committed records), and on a transactional
		// producer they are abortable (the caller must abort, not retry). Both
		// must surface to the caller — matching the Java, librdkafka, and
		// franz-go idempotent-producer fatal-error contract.
		return false
	}
}

// ErrorDetail returns structured fields for JSON/ECS logging and APM tools.
func (e *KafkaError) ErrorDetail() map[string]any {
	out := map[string]any{
		"kafka.error_code": int(e.Code),
		"retriable":        e.Retriable(),
	}
	if e.Topic != "" {
		out["kafka.topic"] = e.Topic
		out["kafka.partition"] = e.Partition
	}
	return out
}

func newKafkaError(code int16, topic string, part int32, msg string) *KafkaError {
	return &KafkaError{Code: ErrorCode(code), Topic: topic, Partition: part, Msg: msg}
}

// IsRetriable reports whether an error should be retried.
func IsRetriable(err error) bool {
	if err == nil {
		return false
	}
	var ke *KafkaError
	if AsKafkaError(err, &ke) {
		return ke.Retriable()
	}
	// Transport/connection failures (e.g. a broker dying mid-request) are
	// transient: refreshing metadata and retrying routes to the new leader.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var nerr net.Error
	return errors.As(err, &nerr)
}

// AsKafkaError reports whether err is (or wraps) a *KafkaError.
func AsKafkaError(err error, target **KafkaError) bool {
	return errors.As(err, target)
}
