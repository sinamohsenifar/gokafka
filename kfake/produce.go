package kfake

import (
	"encoding/binary"

	"github.com/sinamohsenifar/gokafka/internal/wire"
)

const errUnknownTopicOrPartition = int16(3)

// Record batch v2 header field offsets (relative to the batch start).
const (
	batchBaseOffsetPos   = 0  // int64
	batchRecordsCountPos = 57 // int32
	batchHeaderMinLen    = 61
)

// batchRecordCount reads the records_count field from a v2 record batch header.
func batchRecordCount(batch []byte) int32 {
	if len(batch) < batchHeaderMinLen {
		return 0
	}
	return int32(binary.BigEndian.Uint32(batch[batchRecordsCountPos:]))
}

// patchBaseOffset returns a copy of the batch with base_offset set to off, so a
// stored batch reports the offset the broker assigned it.
func patchBaseOffset(batch []byte, off int64) []byte {
	out := make([]byte, len(batch))
	copy(out, batch)
	if len(out) >= 8 {
		binary.BigEndian.PutUint64(out[batchBaseOffsetPos:], uint64(off))
	}
	return out
}

// handleProduce decodes Produce v9 (flexible), appends each partition's record
// batch to its log (assigning a base offset), and reports the assigned offsets.
func (b *Broker) handleProduce(_ int, body []byte) ([]byte, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadCompactNullableString(); err != nil { // transactional_id
		return nil, err
	}
	if _, err := buf.ReadInt16(); err != nil { // acks
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // timeout_ms
		return nil, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	type pres struct {
		part int32
		code int16
		base int64
	}
	type tres struct {
		name  string
		parts []pres
	}
	faultCode := b.takeProduceFault()

	results := make([]tres, 0, int(nTopics))
	for i := 1; i < int(nTopics); i++ {
		name, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		tr := tres{name: name}
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			batch, err := buf.ReadCompactBytes()
			if err != nil {
				return nil, err
			}
			if err := skipTags(buf); err != nil { // partition tag
				return nil, err
			}
			code := int16(0)
			base := int64(-1)
			if faultCode != 0 {
				// Injected fault: report the error and do NOT append the batch, so
				// the record is not committed and the client retries.
				code = faultCode
			} else {
				b.store.mu.Lock()
				t := b.store.topics[name]
				if t == nil || int(part) < 0 || int(part) >= len(t.partitions) {
					code = errUnknownTopicOrPartition
				} else {
					pl := t.partitions[part]
					base = pl.leo
					pl.batches = append(pl.batches, patchBaseOffset(batch, base))
					pl.leo += int64(batchRecordCount(batch))
				}
				b.store.mu.Unlock()
			}
			tr.parts = append(tr.parts, pres{part, code, base})
		}
		if err := skipTags(buf); err != nil { // topic tag
			return nil, err
		}
		results = append(results, tr)
	}

	out := wire.NewBuffer(128)
	out.WriteCompactArrayLen(len(results))
	for _, tr := range results {
		out.WriteCompactString(tr.name)
		out.WriteCompactArrayLen(len(tr.parts))
		for _, pr := range tr.parts {
			out.WriteInt32(pr.part)
			out.WriteInt16(pr.code)
			out.WriteInt64(pr.base)             // base_offset
			out.WriteInt64(-1)                  // log_append_time_ms
			out.WriteInt64(0)                   // log_start_offset
			out.WriteCompactArrayLen(0)         // record_errors
			out.WriteCompactNullableString(nil) // error_message
			out.WriteEmptyTagSection()
		}
		out.WriteEmptyTagSection()
	}
	out.WriteInt32(0)          // throttle_time_ms
	out.WriteEmptyTagSection() // response tag
	return out.Bytes(), nil
}
