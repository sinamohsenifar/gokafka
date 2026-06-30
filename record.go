package gokafka

import "time"

// Record is a Kafka record with optional headers.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
	// DeliveryCount is the KIP-932 share-group delivery attempt count for this
	// record: 1 on first delivery, incremented each time it is re-acquired after
	// a Release or acquisition-lock timeout. It is 0 for records from a regular
	// (non-share) consumer. Use it to build dead-letter logic — e.g. Reject a
	// record once DeliveryCount approaches the group's delivery-count limit
	// (default 5).
	DeliveryCount int16
}

// Header is a record header key-value pair.
type Header struct {
	Key   string
	Value []byte
}
