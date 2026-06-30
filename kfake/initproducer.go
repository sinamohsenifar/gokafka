package kfake

import (
	"sync/atomic"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

var producerIDSeq atomic.Int64

// handleInitProducerID decodes InitProducerID v0 (non-flexible) and allocates a
// producer id for the idempotent/transactional producer.
func (b *Broker) handleInitProducerID(_ int, _ []byte) ([]byte, error) {
	id := producerIDSeq.Add(1)
	out := wire.NewBuffer(16)
	out.WriteInt32(0)  // throttle_time_ms
	out.WriteInt16(0)  // error_code
	out.WriteInt64(id) // producer_id
	out.WriteInt16(0)  // producer_epoch
	return out.Bytes(), nil
}
