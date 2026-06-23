package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/compress"
	"github.com/sinamohsenifar/gokafka/internal/wire"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type ProduceRecord struct {
	Topic     string
	Partition int32
	Key       []byte
	Value     []byte
	Headers   [][2][]byte
	Timestamp time.Time
}

type ProduceResult struct {
	Topic     string
	Partition int32
	Offset    int64
	ErrorCode int16
}

// ProduceSettings controls acks, compression, and idempotent producer fields.
type ProduceSettings struct {
	Acks            int16
	TimeoutMs       int32
	Compression     int8
	Transactional   bool
	TransactionalID string // required in produce request body when Transactional is true (v3+)
	ProducerID        int64
	ProducerEpoch     int16
	NextSequence      func(topic string, partition int32) int32
}

func DefaultProduceSettings() ProduceSettings {
	return ProduceSettings{Acks: -1, TimeoutMs: 30000, Compression: 0}
}

func EncodeProduceRequest(records []ProduceRecord, settings ProduceSettings) ([]byte, error) {
	if settings.TimeoutMs <= 0 {
		settings.TimeoutMs = 30000
	}
	if VerProduce >= 9 {
		return encodeProduceRequestFlex(records, settings)
	}
	return encodeProduceRequestLegacy(records, settings)
}

func writeProduceTransactionalID(buf *wire.Buffer, settings ProduceSettings) {
	if settings.Transactional && settings.TransactionalID != "" {
		buf.WriteString(settings.TransactionalID)
		return
	}
	buf.WriteNullableString(nil)
}

func encodeProduceRequestLegacy(records []ProduceRecord, settings ProduceSettings) ([]byte, error) {
	buf := wire.NewBuffer(256)
	writeProduceTransactionalID(buf, settings)
	buf.WriteInt16(settings.Acks)
	buf.WriteInt32(settings.TimeoutMs)
	groups := groupByTopic(records)
	buf.WriteInt32(int32(len(groups)))
	for _, tg := range groups {
		buf.WriteString(tg.topic)
		partGroups := groupByPartition(tg.parts)
		buf.WriteInt32(int32(len(partGroups)))
		for _, pg := range partGroups {
			seq := int32(0)
			if settings.NextSequence != nil {
				seq = settings.NextSequence(pg.records[0].Topic, pg.partition)
			}
			batch, err := encodeRecordBatch(pg.records, settings, seq)
			if err != nil {
				return nil, err
			}
			buf.WriteInt32(pg.partition)
			buf.WriteBytes(batch)
		}
	}
	return buf.Bytes(), nil
}

func encodeProduceRequestFlex(records []ProduceRecord, settings ProduceSettings) ([]byte, error) {
	buf := wire.NewBuffer(256)
	if settings.Transactional && settings.TransactionalID != "" {
		buf.WriteCompactString(settings.TransactionalID)
	} else {
		buf.WriteCompactNullableString(nil)
	}
	buf.WriteInt16(settings.Acks)
	buf.WriteInt32(settings.TimeoutMs)

	groups := groupByTopic(records)
	buf.WriteCompactArrayLen(len(groups))
	for _, tg := range groups {
		buf.WriteCompactString(tg.topic)
		partGroups := groupByPartition(tg.parts)
		buf.WriteCompactArrayLen(len(partGroups))
		for _, pg := range partGroups {
			seq := int32(0)
			if settings.NextSequence != nil {
				seq = settings.NextSequence(pg.records[0].Topic, pg.partition)
			}
			batch, err := encodeRecordBatch(pg.records, settings, seq)
			if err != nil {
				return nil, err
			}
			buf.WriteInt32(pg.partition)
			buf.WriteCompactBytes(batch)
			buf.WriteEmptyTagSection()
		}
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes(), nil
}

func encodeRecordBatch(records []ProduceRecord, settings ProduceSettings, baseSequence int32) ([]byte, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("protocol: empty record batch")
	}
	firstTS := records[0].Timestamp
	if firstTS.IsZero() {
		firstTS = time.Now()
	}
	firstMs := firstTS.UnixMilli()
	maxMs := firstMs
	for _, r := range records {
		t := r.Timestamp
		if t.IsZero() {
			t = time.Now()
		}
		if m := t.UnixMilli(); m > maxMs {
			maxMs = m
		}
	}

	recordsPayload := encodeRecordsPayload(records)
	attributes := int16(0)
	if settings.Transactional {
		attributes |= 0x0010
	}
	if settings.Compression != 0 {
		compressed, err := compress.Compress(settings.Compression, recordsPayload)
		if err != nil {
			return nil, err
		}
		if len(compressed) < len(recordsPayload) {
			recordsPayload = compressed
			attributes |= int16(settings.Compression)
		}
	}

	lastOffsetDelta := int32(len(records) - 1)

	batch := wire.NewBuffer(64 + len(recordsPayload))
	batch.WriteInt64(0) // baseOffset
	batch.WriteInt32(0) // batchLength placeholder
	batch.WriteInt32(-1)
	batch.WriteInt8(2) // magic
	batch.WriteInt32(0) // crc placeholder
	batch.WriteInt16(attributes)
	batch.WriteInt32(lastOffsetDelta)
	batch.WriteInt64(firstMs)
	batch.WriteInt64(maxMs)
	producerID := settings.ProducerID
	producerEpoch := settings.ProducerEpoch
	baseSeq := baseSequence
	if settings.NextSequence == nil {
		producerID = -1
		producerEpoch = -1
		baseSeq = -1
	}
	batch.WriteInt64(producerID)
	batch.WriteInt16(producerEpoch)
	batch.WriteInt32(baseSeq)
	batch.WriteInt32(int32(len(records))) // numRecords (KIP-107)
	batch.B = append(batch.B, recordsPayload...)

	body := batch.Bytes()
	batchLen := int32(len(body) - 12)
	binary.BigEndian.PutUint32(body[8:12], uint32(batchLen))
	crc := crc32.Checksum(body[21:], castagnoliTable)
	binary.BigEndian.PutUint32(body[17:21], crc)
	return body, nil
}

func encodeRecordsPayload(records []ProduceRecord) []byte {
	buf := wire.NewBuffer(64)
	firstMs := records[0].Timestamp
	if firstMs.IsZero() {
		firstMs = time.Now()
	}
	firstMsVal := firstMs.UnixMilli()
	for i, pr := range records {
		ts := pr.Timestamp
		if ts.IsZero() {
			ts = time.Now()
		}
		rec := encodeSingleRecord(pr.Key, pr.Value, pr.Headers, int64(i), ts.UnixMilli()-firstMsVal)
		buf.WriteVarint(len(rec))
		buf.B = append(buf.B, rec...)
	}
	return buf.Bytes()
}

func encodeSingleRecord(key, value []byte, headers [][2][]byte, offsetDelta, timestampDelta int64) []byte {
	buf := wire.NewBuffer(64 + len(key) + len(value))
	buf.WriteInt8(0) // attributes
	buf.WriteVarint(int(timestampDelta))
	buf.WriteVarint(int(offsetDelta))
	buf.WriteVarint(len(key))
	if len(key) > 0 {
		buf.B = append(buf.B, key...)
	}
	buf.WriteVarint(len(value))
	if len(value) > 0 {
		buf.B = append(buf.B, value...)
	}
	buf.WriteVarint(len(headers))
	for _, h := range headers {
		buf.WriteVarint(len(h[0]))
		if len(h[0]) > 0 {
			buf.B = append(buf.B, h[0]...)
		}
		buf.WriteVarint(len(h[1]))
		if len(h[1]) > 0 {
			buf.B = append(buf.B, h[1]...)
		}
	}
	return buf.Bytes()
}

type topicParts struct {
	topic string
	parts []ProduceRecord
}

type partitionGroup struct {
	partition int32
	records   []ProduceRecord
}

func groupByPartition(records []ProduceRecord) []partitionGroup {
	order := make([]int32, 0)
	m := map[int32][]ProduceRecord{}
	for _, r := range records {
		if _, ok := m[r.Partition]; !ok {
			order = append(order, r.Partition)
		}
		m[r.Partition] = append(m[r.Partition], r)
	}
	out := make([]partitionGroup, 0, len(order))
	for _, p := range order {
		out = append(out, partitionGroup{partition: p, records: m[p]})
	}
	return out
}

func groupByTopic(records []ProduceRecord) []topicParts {
	order := make([]string, 0)
	m := map[string][]ProduceRecord{}
	for _, r := range records {
		if _, ok := m[r.Topic]; !ok {
			order = append(order, r.Topic)
		}
		m[r.Topic] = append(m[r.Topic], r)
	}
	out := make([]topicParts, 0, len(order))
	for _, t := range order {
		out = append(out, topicParts{topic: t, parts: m[t]})
	}
	return out
}

func DecodeProduceResponse(body []byte) ([]ProduceResult, error) {
	if VerProduce >= 9 {
		return decodeProduceResponseFlex(body)
	}
	return decodeProduceResponseLegacy(body)
}

func decodeProduceResponseLegacy(body []byte) ([]ProduceResult, error) {
	buf := wire.FromBytes(body)
	nTopics, err := buf.ReadInt32()
	if err != nil {
		return nil, err
	}
	var out []ProduceResult
	for i := 0; i < int(nTopics); i++ {
		name, err := buf.ReadString()
		if err != nil {
			return nil, err
		}
		nParts, err := buf.ReadInt32()
		if err != nil {
			return nil, err
		}
		for j := 0; j < int(nParts); j++ {
			part, err := buf.ReadInt32()
			if err != nil {
				return nil, err
			}
			errCode, err := buf.ReadInt16()
			if err != nil {
				return nil, err
			}
			offset, err := buf.ReadInt64()
			if err != nil {
				return nil, err
			}
			if _, err := buf.ReadInt64(); err != nil { // log_append_time_ms
				return nil, err
			}
			if _, err := buf.ReadInt64(); err != nil { // log_start_offset
				return nil, err
			}
			out = append(out, ProduceResult{Topic: name, Partition: part, Offset: offset, ErrorCode: errCode})
		}
	}
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return nil, err
	}
	return out, nil
}

func decodeProduceResponseFlex(body []byte) ([]ProduceResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil {
		return nil, err
	}
	nTopics, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	var out []ProduceResult
	for i := 1; i < int(nTopics); i++ {
		name, err := buf.ReadCompactString()
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
			offset, err := buf.ReadInt64()
			if err != nil {
				return nil, err
			}
			if _, err := buf.ReadInt64(); err != nil {
				return nil, err
			}
			if _, err := buf.ReadInt64(); err != nil {
				return nil, err
			}
			out = append(out, ProduceResult{Topic: name, Partition: part, Offset: offset, ErrorCode: errCode})
			if err := buf.SkipTagSection(); err != nil {
				return nil, err
			}
		}
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}
