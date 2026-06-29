package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// Quota component match types (KIP-546).
const (
	QuotaMatchExact   int8 = 0 // match the exact entity name in Match
	QuotaMatchDefault int8 = 1 // match the default entity
	QuotaMatchAny     int8 = 2 // match any value of the entity type
)

// QuotaComponent is one filter clause of a DescribeClientQuotas request.
type QuotaComponent struct {
	EntityType string
	MatchType  int8
	Match      *string // entity name for QuotaMatchExact; nil otherwise
}

// QuotaEntityElem is one (type, name) pair of a quota entity. A nil Name means
// the default entity for that type.
type QuotaEntityElem struct {
	Type string
	Name *string
}

// QuotaEntry is a described quota entity with its configured values.
type QuotaEntry struct {
	Entity []QuotaEntityElem
	Values map[string]float64
}

// QuotaOp mutates a single quota key. Remove deletes the key; otherwise Value is set.
type QuotaOp struct {
	Key    string
	Value  float64
	Remove bool
}

// QuotaAlteration applies a set of ops to one entity.
type QuotaAlteration struct {
	Entity []QuotaEntityElem
	Ops    []QuotaOp
}

// QuotaAlterResult is the per-entity outcome of AlterClientQuotas.
type QuotaAlterResult struct {
	Entity       []QuotaEntityElem
	ErrorCode    int16
	ErrorMessage string
}

func EncodeDescribeClientQuotasRequest(version int16, components []QuotaComponent, strict bool) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactArrayLen(len(components))
	for _, c := range components {
		buf.WriteCompactString(c.EntityType)
		buf.WriteInt8(c.MatchType)
		buf.WriteCompactNullableString(c.Match)
		buf.WriteEmptyTagSection()
	}
	buf.WriteBool(strict)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeDescribeClientQuotasResponse(version int16, body []byte) (int16, string, []QuotaEntry, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle
		return 0, "", nil, err
	}
	code, err := buf.ReadInt16()
	if err != nil {
		return 0, "", nil, err
	}
	msg, err := buf.ReadCompactNullableString()
	if err != nil {
		return code, "", nil, err
	}
	nEntries, err := buf.ReadUvarint()
	if err != nil {
		return code, msg, nil, err
	}
	out := make([]QuotaEntry, 0, safePrealloc(int(nEntries)-1))
	for i := 1; i < int(nEntries); i++ {
		var e QuotaEntry
		nEnt, err := buf.ReadUvarint()
		if err != nil {
			return code, msg, nil, err
		}
		for j := 1; j < int(nEnt); j++ {
			el, err := readQuotaEntity(buf)
			if err != nil {
				return code, msg, nil, err
			}
			e.Entity = append(e.Entity, el)
		}
		nVals, err := buf.ReadUvarint()
		if err != nil {
			return code, msg, nil, err
		}
		if nVals > 1 {
			e.Values = make(map[string]float64, int(nVals)-1)
		}
		for j := 1; j < int(nVals); j++ {
			key, err := buf.ReadCompactString()
			if err != nil {
				return code, msg, nil, err
			}
			val, err := buf.ReadFloat64()
			if err != nil {
				return code, msg, nil, err
			}
			if err := buf.SkipTagSection(); err != nil {
				return code, msg, nil, err
			}
			e.Values[key] = val
		}
		if err := buf.SkipTagSection(); err != nil {
			return code, msg, nil, err
		}
		out = append(out, e)
	}
	if err := buf.SkipTagSection(); err != nil {
		return code, msg, nil, err
	}
	return code, msg, out, nil
}

func EncodeAlterClientQuotasRequest(version int16, alterations []QuotaAlteration, validateOnly bool) []byte {
	buf := wire.NewBuffer(64)
	buf.WriteCompactArrayLen(len(alterations))
	for _, a := range alterations {
		buf.WriteCompactArrayLen(len(a.Entity))
		for _, el := range a.Entity {
			buf.WriteCompactString(el.Type)
			buf.WriteCompactNullableString(el.Name)
			buf.WriteEmptyTagSection()
		}
		buf.WriteCompactArrayLen(len(a.Ops))
		for _, op := range a.Ops {
			buf.WriteCompactString(op.Key)
			buf.WriteFloat64(op.Value)
			buf.WriteBool(op.Remove)
			buf.WriteEmptyTagSection()
		}
		buf.WriteEmptyTagSection()
	}
	buf.WriteBool(validateOnly)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

func DecodeAlterClientQuotasResponse(version int16, body []byte) ([]QuotaAlterResult, error) {
	buf := wire.FromBytes(body)
	if _, err := buf.ReadInt32(); err != nil { // throttle
		return nil, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return nil, err
	}
	out := make([]QuotaAlterResult, 0, safePrealloc(int(n)-1))
	for i := 1; i < int(n); i++ {
		var r QuotaAlterResult
		if r.ErrorCode, err = buf.ReadInt16(); err != nil {
			return nil, err
		}
		if r.ErrorMessage, err = buf.ReadCompactNullableString(); err != nil {
			return nil, err
		}
		nEnt, err := buf.ReadUvarint()
		if err != nil {
			return nil, err
		}
		for j := 1; j < int(nEnt); j++ {
			el, err := readQuotaEntity(buf)
			if err != nil {
				return nil, err
			}
			r.Entity = append(r.Entity, el)
		}
		if err := buf.SkipTagSection(); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := buf.SkipTagSection(); err != nil {
		return nil, err
	}
	return out, nil
}

func readQuotaEntity(buf *wire.Buffer) (QuotaEntityElem, error) {
	var el QuotaEntityElem
	t, err := buf.ReadCompactString()
	if err != nil {
		return el, err
	}
	el.Type = t
	name, isNull, err := buf.ReadCompactNullableStringPtr()
	if err != nil {
		return el, err
	}
	if !isNull {
		el.Name = &name
	}
	if err := buf.SkipTagSection(); err != nil {
		return el, err
	}
	return el, nil
}
