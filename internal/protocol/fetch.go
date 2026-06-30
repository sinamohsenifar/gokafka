package protocol

import (
	"encoding/binary"
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/compress"
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

type FetchPartition struct {
	Topic       string
	TopicID     wire.UUID // topic_id for Fetch v13+ (KIP-516); zero = resolve by name
	Partition   int32
	Offset      int64
	LeaderEpoch int32 // current_leader_epoch (-1 = unknown / no fencing)
	MaxBytes    int32
}

type FetchedRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   [][2][]byte
	Timestamp int64
	// Control marks a transaction commit/abort marker. Such records carry only
	// an offset (so the consumer can advance past them) and must not be
	// delivered to the application.
	Control bool
	// DeliveryCount is the KIP-932 share-group delivery attempt count, assigned
	// from the ShareFetch acquired-records ranges (0 for regular fetches).
	DeliveryCount int16
}

func EncodeFetchRequest(ver int16, _ string, partitions []FetchPartition, maxWaitMs int32, minBytes, maxBytes int32, isolationLevel int8) []byte {
	if ver >= 12 {
		return encodeFetchRequestFlex(ver, partitions, maxWaitMs, minBytes, maxBytes, isolationLevel)
	}
	return encodeFetchRequestLegacy(ver, partitions, maxWaitMs, minBytes, maxBytes, isolationLevel)
}

func encodeFetchRequestLegacy(ver int16, partitions []FetchPartition, maxWaitMs, minBytes, maxBytes int32, isolationLevel int8) []byte {
	buf := wire.NewBuffer(128)
	buf.WriteInt32(-1)
	buf.WriteInt32(maxWaitMs)
	buf.WriteInt32(minBytes)
	buf.WriteInt32(maxBytes)
	if ver >= 4 {
		buf.WriteInt8(isolationLevel)
	}
	if ver >= 7 {
		buf.WriteInt32(0)
		buf.WriteInt32(0)
	}

	topics := map[string][]FetchPartition{}
	order := make([]string, 0)
	for _, p := range partitions {
		if _, ok := topics[p.Topic]; !ok {
			order = append(order, p.Topic)
		}
		topics[p.Topic] = append(topics[p.Topic], p)
	}
	buf.WriteInt32(int32(len(order)))
	for _, topic := range order {
		buf.WriteString(topic)
		parts := topics[topic]
		buf.WriteInt32(int32(len(parts)))
		for _, p := range parts {
			buf.WriteInt32(p.Partition)
			if ver >= 9 {
				buf.WriteInt32(p.LeaderEpoch) // current_leader_epoch
			}
			buf.WriteInt64(p.Offset)
			if ver >= 5 {
				buf.WriteInt64(-1)
			}
			maxB := p.MaxBytes
			if maxB <= 0 {
				maxB = 1 << 20
			}
			buf.WriteInt32(maxB)
		}
	}
	if ver >= 10 {
		buf.WriteInt32(0) // forgotten_topics_data
	}
	if ver >= 11 {
		buf.WriteString("") // rack_id
	}
	return buf.Bytes()
}

func encodeFetchRequestFlex(ver int16, partitions []FetchPartition, maxWaitMs, minBytes, maxBytes int32, isolationLevel int8) []byte {
	buf := wire.NewBuffer(128)
	buf.WriteInt32(-1)
	buf.WriteInt32(maxWaitMs)
	buf.WriteInt32(minBytes)
	buf.WriteInt32(maxBytes)
	buf.WriteInt8(isolationLevel)
	buf.WriteInt32(0)  // session_id
	buf.WriteInt32(-1) // session_epoch

	topics := map[string][]FetchPartition{}
	order := make([]string, 0)
	for _, p := range partitions {
		if _, ok := topics[p.Topic]; !ok {
			order = append(order, p.Topic)
		}
		topics[p.Topic] = append(topics[p.Topic], p)
	}
	buf.WriteCompactArrayLen(len(order))
	for _, topic := range order {
		parts := topics[topic]
		if ver >= 13 {
			// KIP-516: identify the topic by its UUID instead of its name.
			buf.WriteUUID(parts[0].TopicID)
		} else {
			buf.WriteCompactString(topic)
		}
		buf.WriteCompactArrayLen(len(parts))
		for _, p := range parts {
			buf.WriteInt32(p.Partition)
			buf.WriteInt32(p.LeaderEpoch) // current_leader_epoch
			buf.WriteInt64(p.Offset)
			if ver >= 12 {
				buf.WriteInt32(-1) // last_fetched_epoch
			}
			if ver >= 5 {
				buf.WriteInt64(-1) // log_start_offset
			}
			maxB := p.MaxBytes
			if maxB <= 0 {
				maxB = 1 << 20
			}
			buf.WriteInt32(maxB)
			buf.WriteEmptyTagSection()
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteCompactArrayLen(0) // forgotten_topics_data
	if ver >= 11 {
		buf.WriteCompactString("")
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeFetchResponse decodes a Fetch response. For v13+ (KIP-516) topics are
// identified by UUID, so resolveTopic maps a topic id back to its name; it may
// be nil for older versions. An unresolvable id yields ErrUnknownTopicID so the
// caller can refresh metadata and retry.
func DecodeFetchResponse(ver int16, body []byte, resolveTopic func(wire.UUID) (string, bool)) ([]FetchedRecord, error) {
	if ver >= 12 {
		return decodeFetchResponseFlex(ver, body, resolveTopic)
	}
	return decodeFetchResponseLegacy(ver, body)
}

func decodeFetchResponseLegacy(ver int16, body []byte) ([]FetchedRecord, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	if ver >= 7 {
		if _, err := buf.ReadInt16(); err != nil {
			return nil, err
		}
		if _, err := buf.ReadInt32(); err != nil {
			return nil, err
		}
	}
	nTopics, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	var out []FetchedRecord
	for i := int32(0); i < nTopics; i++ {
		topic, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		for j := int32(0); j < nParts; j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			errCode, err := buf.ReadInt16()
			if err != nil {
				return nil, err
			}
			if errCode == 27 {
				return nil, ErrRebalanceInProgress
			}
			if errCode == 6 || errCode == 74 || errCode == 75 {
				return nil, ErrLeaderEpochChanged
			}
			if errCode == 100 { // UNKNOWN_TOPIC_ID
				return nil, ErrUnknownTopicID
			}
			if errCode != 0 {
				return nil, fmt.Errorf("protocol: fetch partition error %d", errCode)
			}
			if _, err := buf.ReadInt64(); err != nil { // high_watermark
				return nil, err
			}
			if ver >= 4 {
				if _, err := buf.ReadInt64(); err != nil { // last_stable_offset
					return nil, err
				}
			}
			if ver >= 5 {
				if _, err := buf.ReadInt64(); err != nil { // log_start_offset
					return nil, err
				}
			}
			var aborted []abortedTxn
			if ver >= 4 {
				aborted, err = readAbortedTransactions(buf)
				if err != nil {
					return nil, err
				}
			}
			if ver >= 11 {
				if _, err := buf.ReadInt32(); err != nil { // preferred_read_replica
					return nil, err
				}
			}
			records, err := buf.ReadBytes()
			if err != nil {
				return nil, err
			}
			recs, err := decodeRecordBatch(topic, part, records, aborted)
			if err != nil {
				return nil, err
			}
			out = append(out, recs...)
		}
	}
	return out, nil
}

func readAbortedTransactions(buf *wire.Buffer) ([]abortedTxn, error) {
	n, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}
	out := make([]abortedTxn, 0, safePrealloc(int(n)))
	for i := int32(0); i < n; i++ {
		pid, err := buf.ReadInt64()
		if err != nil {
			return nil, err
		}
		fo, err := buf.ReadInt64()
		if err != nil {
			return nil, err
		}
		out = append(out, abortedTxn{producerID: pid, firstOffset: fo})
	}
	return out, nil
}

func decodeFetchResponseFlex(ver int16, body []byte, resolveTopic func(wire.UUID) (string, bool)) ([]FetchedRecord, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return nil, err
	}
	if ver >= 7 {
		if _, err := buf.ReadInt16(); err != nil { // error_code
			return nil, err
		}
		if _, err := buf.ReadInt32(); err != nil { // session_id
			return nil, err
		}
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	var out []FetchedRecord
	for i := 1; i < int(nTopics); i++ {
		var topic string
		if ver >= 13 {
			// KIP-516: the topic is identified by UUID; map it back to a name.
			tid, err := buf.ReadUUID()
			if err != nil {
				return nil, err
			}
			name, ok := resolveTopic(tid)
			if !ok {
				return nil, ErrUnknownTopicID
			}
			topic = name
		} else {
			topic, err = buf.ReadCompactString()
			if err != nil {
				return nil, err
			}
		}
		nParts, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		for j := 1; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			errCode, err := buf.ReadInt16()
			if err != nil {
				return nil, err
			}
			if errCode == 27 {
				return nil, ErrRebalanceInProgress
			}
			if errCode == 6 || errCode == 74 || errCode == 75 {
				return nil, ErrLeaderEpochChanged
			}
			if errCode == 100 { // UNKNOWN_TOPIC_ID
				return nil, ErrUnknownTopicID
			}
			if errCode != 0 {
				return nil, fmt.Errorf("protocol: fetch partition error %d", errCode)
			}
			if _, err := buf.ReadInt64(); err != nil { // high_watermark
				return nil, err
			}
			if ver >= 4 {
				if _, err := buf.ReadInt64(); err != nil { // last_stable_offset
					return nil, err
				}
			}
			if ver >= 5 {
				if _, err := buf.ReadInt64(); err != nil { // log_start_offset
					return nil, err
				}
			}
			var aborted []abortedTxn
			if ver >= 4 {
				aborted, err = readAbortedTransactionsFlex(buf)
				if err != nil {
					return nil, err
				}
			}
			if ver >= 11 {
				if _, err := buf.ReadInt32(); err != nil { // preferred_read_replica
					return nil, err
				}
			}
			records, err := buf.ReadCompactBytes()
			if err != nil {
				return nil, err
			}
			recs, err := decodeRecordBatch(topic, part, records, aborted)
			if err != nil {
				return nil, err
			}
			out = append(out, recs...)
			if err := buf.SkipTagSection(); err != nil {
				return nil, err
			}
		}
		if err := buf.SkipTagSection(); err != nil { // topic tags
			return nil, err
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

func readAbortedTransactionsFlex(buf *wire.Buffer) ([]abortedTxn, error) {
	n, err := buf.ReadUvarint()
	if err != nil || n <= 1 {
		return nil, err
	}
	out := make([]abortedTxn, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		pid, err := buf.ReadInt64()
		if err != nil {
			return nil, err
		}
		fo, err := buf.ReadInt64()
		if err != nil {
			return nil, err
		}
		if err := buf.SkipTagSection(); err != nil { // per-element tagged fields (flex)
			return nil, err
		}
		out = append(out, abortedTxn{producerID: pid, firstOffset: fo})
	}
	return out, nil
}

// abortedTxn is one entry of a Fetch response's aborted-transactions list.
type abortedTxn struct {
	producerID  int64
	firstOffset int64
}

// decodeRecordBatch decodes a partition's record-batch blob, applying
// read_committed filtering using the broker's aborted-transactions list
// (empty under read_uncommitted). Records from aborted transactions and
// transaction control markers are dropped, but a control marker carrying the
// batch's last offset is emitted so the consumer still advances past them.
func decodeRecordBatch(topic string, part int32, data []byte, aborted []abortedTxn) ([]FetchedRecord, error) {
	var out []FetchedRecord
	var abortedPIDs map[int64]bool
	ai := 0
	for len(data) >= 12 {
		batchLen := int(binary.BigEndian.Uint32(data[8:12]))
		total := 12 + batchLen
		if batchLen < 0 || total > len(data) {
			break
		}
		info, err := decodeOneRecordBatch(topic, part, data[:total])
		if err != nil {
			return nil, err
		}
		data = data[total:]
		if info == nil {
			continue
		}
		// Transactions whose first offset is at or before this batch are now
		// known to be aborted (the list is ordered by first offset).
		for ai < len(aborted) && aborted[ai].firstOffset <= info.baseOffset {
			if abortedPIDs == nil {
				abortedPIDs = make(map[int64]bool)
			}
			abortedPIDs[aborted[ai].producerID] = true
			ai++
		}
		switch {
		case info.isControl:
			// Commit/abort marker ends the producer's transaction; never delivered.
			delete(abortedPIDs, info.producerID)
			out = append(out, FetchedRecord{Topic: topic, Partition: part, Offset: info.lastOffset, Control: true})
		case info.isTransactional && abortedPIDs[info.producerID]:
			// Records from an aborted transaction: drop, but advance past them.
			out = append(out, FetchedRecord{Topic: topic, Partition: part, Offset: info.lastOffset, Control: true})
		default:
			out = append(out, info.records...)
		}
	}
	return out, nil
}

// recordBatchInfo is a decoded record batch plus the metadata needed for
// read_committed filtering.
type recordBatchInfo struct {
	baseOffset      int64
	lastOffset      int64
	producerID      int64
	isControl       bool
	isTransactional bool
	records         []FetchedRecord
}

func decodeOneRecordBatch(topic string, part int32, batch []byte) (*recordBatchInfo, error) {
	if len(batch) < 65 {
		return nil, nil
	}
	buf := wire.FromBytes(batch)
	baseOffset, err := buf.ReadInt64() // baseOffset
	if err != nil {
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // batchLength
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // partitionLeaderEpoch
		return nil, err
	}
	magic, err := buf.ReadInt8() // magic
	if err != nil {
		return nil, err
	}
	if magic != 2 {
		// v0/v1 message sets have an entirely different layout; parsing them as
		// a v2 RecordBatch yields garbage records. Brokers >= 0.11 (well below
		// our 3.4+ target) always send v2, so reject anything else.
		return nil, fmt.Errorf("protocol: unsupported record batch magic %d (expected 2)", magic)
	}
	if _, err := buf.ReadInt32(); err != nil { // crc
		return nil, err
	}
	attributes, err := buf.ReadInt16()
	if err != nil {
		return nil, err
	}
	lastOffsetDelta, err := buf.ReadInt32() // lastOffsetDelta
	if err != nil {
		return nil, err
	}
	firstTimestamp, err := buf.ReadInt64()
	if err != nil {
		return nil, err
	}
	if _, err := buf.ReadInt64(); err != nil { // maxTimestamp
		return nil, err
	}
	producerID, err := buf.ReadInt64() // producerId
	if err != nil {
		return nil, err
	}
	if _, err := buf.ReadInt16(); err != nil { // producerEpoch
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // baseSequence
		return nil, err
	}
	if _, err := buf.ReadInt32(); err != nil { // numRecords
		return nil, err
	}
	info := &recordBatchInfo{
		baseOffset:      baseOffset,
		lastOffset:      baseOffset + int64(lastOffsetDelta),
		producerID:      producerID,
		isControl:       attributes&0x20 != 0,
		isTransactional: attributes&0x10 != 0,
	}
	if info.isControl {
		// Control batch (commit/abort marker): not parsed or delivered.
		return info, nil
	}
	recordsBytes := buf.Remaining()
	codec := int8(attributes & 0x7)
	if codec != 0 {
		recordsBytes, err = compress.Decompress(codec, recordsBytes)
		if err != nil {
			return nil, err
		}
	}
	recs, err := parseRecords(topic, part, recordsBytes, baseOffset, firstTimestamp)
	if err != nil {
		return nil, err
	}
	info.records = recs
	return info, nil
}

func parseRecords(topic string, part int32, data []byte, baseOffset, baseTimestamp int64) ([]FetchedRecord, error) {
	buf := wire.FromBytes(data)
	var out []FetchedRecord
	for len(buf.Remaining()) > 0 {
		length, err := buf.ReadVarint()
		if err != nil {
			return nil, err
		}
		if length <= 0 {
			break
		}
		recordEnd := buf.I + int(length)
		if recordEnd > len(buf.B) {
			break
		}
		if _, err := buf.ReadInt8(); err != nil { // per-record attributes (unused)
			return nil, err
		}
		tsDelta, err := buf.ReadVarint()
		if err != nil {
			return nil, err
		}
		offsetDelta, err := buf.ReadVarint()
		if err != nil {
			return nil, err
		}
		keyLen, err := buf.ReadVarint()
		if err != nil {
			return nil, err
		}
		var key []byte
		if keyLen > 0 {
			if int(keyLen) > len(buf.Remaining()) {
				buf.I = recordEnd
				continue
			}
			key = make([]byte, keyLen)
			copy(key, buf.B[buf.I:buf.I+int(keyLen)])
			buf.I += int(keyLen)
		}
		valLen, err := buf.ReadVarint()
		if err != nil {
			return nil, err
		}
		var val []byte
		if valLen > 0 {
			if int(valLen) > len(buf.Remaining()) {
				buf.I = recordEnd
				continue
			}
			val = make([]byte, valLen)
			copy(val, buf.B[buf.I:buf.I+int(valLen)])
			buf.I += int(valLen)
		}
		nHdrs, err := buf.ReadVarint()
		if err != nil {
			return nil, err
		}
		var hdrs [][2][]byte
		for h := 0; h < nHdrs; h++ {
			kLen, err := buf.ReadVarint()
			if err != nil {
				return nil, err
			}
			var k []byte
			if kLen > 0 {
				if int(kLen) > len(buf.Remaining()) {
					buf.I = recordEnd
					break
				}
				k = make([]byte, kLen)
				copy(k, buf.B[buf.I:buf.I+int(kLen)])
				buf.I += int(kLen)
			}
			vLen, err := buf.ReadVarint()
			if err != nil {
				return nil, err
			}
			var v []byte
			if vLen > 0 {
				if int(vLen) > len(buf.Remaining()) {
					buf.I = recordEnd
					break
				}
				v = make([]byte, vLen)
				copy(v, buf.B[buf.I:buf.I+int(vLen)])
				buf.I += int(vLen)
			}
			hdrs = append(hdrs, [2][]byte{k, v})
		}
		if buf.I != recordEnd {
			buf.I = recordEnd
		}
		out = append(out, FetchedRecord{
			Topic: topic, Partition: part, Offset: baseOffset + int64(offsetDelta),
			Key: key, Value: val, Headers: hdrs, Timestamp: baseTimestamp + int64(tsDelta),
		})
	}
	return out, nil
}
