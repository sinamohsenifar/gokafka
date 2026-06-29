package protocol

import (
	"encoding/binary"
	"fmt"

	"github.com/sinamohsenifar/gokafka/internal/compress"
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

type FetchPartition struct {
	Topic     string
	Partition int32
	Offset    int64
	MaxBytes  int32
}

type FetchedRecord struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   [][2][]byte
	Timestamp int64
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
				buf.WriteInt32(-1)
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
		buf.WriteCompactString(topic)
		parts := topics[topic]
		buf.WriteCompactArrayLen(len(parts))
		for _, p := range parts {
			buf.WriteInt32(p.Partition)
			buf.WriteInt32(-1) // current_leader_epoch
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

func DecodeFetchResponse(ver int16, body []byte) ([]FetchedRecord, error) {
	if ver >= 12 {
		return decodeFetchResponseFlex(ver, body)
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
			if ver >= 4 {
				if err := skipAbortedTransactions(buf); err != nil {
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
			recs, err := decodeRecordBatch(topic, part, records)
			if err != nil {
				return nil, err
			}
			out = append(out, recs...)
		}
	}
	return out, nil
}

func skipAbortedTransactions(buf *wire.Buffer) error {
	n, err := buf.ReadInt32()
	if err != nil {
		return err
	}
	for i := int32(0); i < n; i++ {
		if _, err := buf.ReadInt64(); err != nil {
			return err
		}
		if _, err := buf.ReadInt64(); err != nil {
			return err
		}
	}
	return nil
}

func decodeFetchResponseFlex(ver int16, body []byte) ([]FetchedRecord, error) {
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
		topic, err := buf.ReadCompactString()
		if err != nil {
			return nil, err
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
			if ver >= 4 {
				if err := skipAbortedTransactionsFlex(buf); err != nil {
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
			recs, err := decodeRecordBatch(topic, part, records)
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

func skipAbortedTransactionsFlex(buf *wire.Buffer) error {
	n, err := buf.ReadUvarint()
	if err != nil || n == 0 {
		return err
	}
	for i := 1; i < int(n); i++ {
		if _, err := buf.ReadInt64(); err != nil {
			return err
		}
		if _, err := buf.ReadInt64(); err != nil {
			return err
		}
	}
	return nil
}

func decodeRecordBatch(topic string, part int32, data []byte) ([]FetchedRecord, error) {
	var out []FetchedRecord
	for len(data) >= 12 {
		batchLen := int(binary.BigEndian.Uint32(data[8:12]))
		total := 12 + batchLen
		if batchLen < 0 || total > len(data) {
			break
		}
		recs, err := decodeOneRecordBatch(topic, part, data[:total])
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
		data = data[total:]
	}
	return out, nil
}

func decodeOneRecordBatch(topic string, part int32, batch []byte) ([]FetchedRecord, error) {
	if len(batch) < 65 {
		return nil, nil
	}
	buf := wire.FromBytes(batch)
	if _, err := buf.ReadInt64(); err != nil { // baseOffset
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
	if _, err := buf.ReadInt32(); err != nil { // lastOffsetDelta
		return nil, err
	}
	firstTimestamp, err := buf.ReadInt64()
	if err != nil {
		return nil, err
	}
	if _, err := buf.ReadInt64(); err != nil { // maxTimestamp
		return nil, err
	}
	if _, err := buf.ReadInt64(); err != nil { // producerId
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
	recordsBytes := buf.Remaining()
	codec := int8(attributes & 0x7)
	if codec != 0 {
		var err error
		recordsBytes, err = compress.Decompress(codec, recordsBytes)
		if err != nil {
			return nil, err
		}
	}
	return parseRecords(topic, part, recordsBytes, firstTimestamp)
}

func parseRecords(topic string, part int32, data []byte, baseTimestamp int64) ([]FetchedRecord, error) {
	buf := wire.FromBytes(data)
	var out []FetchedRecord
	var baseOffset int64
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
		attrs, err := buf.ReadInt8()
		if err != nil {
			return nil, err
		}
		if attrs&0x03 == 3 { // control record (commit/abort marker)
			buf.I = recordEnd
			continue
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
		offset := baseOffset + int64(offsetDelta)
		if len(out) == 0 {
			baseOffset = offset
		}
		out = append(out, FetchedRecord{
			Topic: topic, Partition: part, Offset: offset,
			Key: key, Value: val, Headers: hdrs, Timestamp: baseTimestamp + int64(tsDelta),
		})
	}
	return out, nil
}
