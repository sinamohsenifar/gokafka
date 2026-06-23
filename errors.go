package gokafka

import (
	"errors"
	"fmt"
)

var (
	ErrNoBrokers          = errors.New("gokafka: at least one broker is required")
	ErrClosed             = errors.New("gokafka: client is closed")
	ErrNoConsumerGroup    = errors.New("gokafka: consumer group is required")
	ErrNoShareGroup       = errors.New("gokafka: share group is required")
	ErrNoSchemaURL        = errors.New("gokafka: schema registry URL is required")
	ErrTopicNotFound      = errors.New("gokafka: topic not found")
	ErrNoTransactionalID  = errors.New("gokafka: transactional id is required when transactions are enabled")
	ErrTransactionAborted = errors.New("gokafka: transaction aborted")
	ErrRetriable            = errors.New("gokafka: retriable error")
	ErrInvalidProducerConfig = errors.New("gokafka: idempotent producer requires acks=all")
	ErrUnsupportedCompression = errors.New("gokafka: zstd compression is not supported (stdlib-only client)")
)

// ErrorCode mirrors Kafka protocol error codes.
type ErrorCode int16

const (
	ErrCodeNone              ErrorCode = 0
	ErrCodeUnknownTopic      ErrorCode = 3
	ErrCodeLeaderNotAvail    ErrorCode = 5
	ErrCodeNotLeaderForPart  ErrorCode = 6
	ErrCodeRequestTimedOut   ErrorCode = 7
	ErrCodeNetworkException  ErrorCode = 8
	ErrCodeCoordinatorLoad          ErrorCode = 14
	ErrCodeCoordinatorNotAvailable    ErrorCode = 15
	ErrCodeNotCoordinator             ErrorCode = 16
	ErrCodeNotEnoughReplicas          ErrorCode = 19
	ErrCodeNotEnoughReplicasAfterAppend ErrorCode = 20
	ErrCodeRebalanceInProg   ErrorCode = 27
	ErrCodeInvalidTxnState      ErrorCode = 37
	ErrCodeOutOfOrderSequence   ErrorCode = 45
	ErrCodeInvalidProducerEpoch ErrorCode = 47
)

// KafkaError wraps a broker error code with context.
type KafkaError struct {
	Code    ErrorCode
	Topic   string
	Partition int32
	Msg     string
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
		ErrCodeInvalidProducerEpoch, ErrCodeOutOfOrderSequence:
		return true
	default:
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
	var ke *KafkaError
	if AsKafkaError(err, &ke) {
		return ke.Retriable()
	}
	return false
}

// AsKafkaError reports whether err is a *KafkaError.
func AsKafkaError(err error, target **KafkaError) bool {
	if err == nil {
		return false
	}
	if ke, ok := err.(*KafkaError); ok {
		*target = ke
		return true
	}
	return false
}
