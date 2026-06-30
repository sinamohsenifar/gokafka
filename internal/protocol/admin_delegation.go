package protocol

import "github.com/sinamohsenifar/gokafka/internal/wire"

// Principal is a Kafka principal (type + name), e.g. {"User", "alice"}.
type Principal struct {
	Type string
	Name string
}

// DelegationToken describes a delegation token returned by Create/Describe.
type DelegationToken struct {
	Owner           Principal
	TokenRequester  Principal
	IssueTimestamp  int64
	ExpiryTimestamp int64
	MaxTimestamp    int64
	TokenID         string
	HMAC            []byte
	Renewers        []Principal
}

// EncodeCreateDelegationTokenRequest encodes API 38 (flexible v3).
func EncodeCreateDelegationTokenRequest(owner *Principal, renewers []Principal, maxLifetimeMs int64) []byte {
	buf := wire.NewBuffer(64)
	if owner != nil {
		buf.WriteCompactString(owner.Type)
		buf.WriteCompactString(owner.Name)
	} else {
		buf.WriteUvarint(0) // null owner_principal_type
		buf.WriteUvarint(0) // null owner_principal_name
	}
	buf.WriteCompactArrayLen(len(renewers))
	for _, r := range renewers {
		buf.WriteCompactString(r.Type)
		buf.WriteCompactString(r.Name)
		buf.WriteEmptyTagSection()
	}
	buf.WriteInt64(maxLifetimeMs)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeCreateDelegationTokenResponse decodes API 38 response (flexible v3).
func DecodeCreateDelegationTokenResponse(body []byte) (int16, DelegationToken, error) {
	buf := wire.FromBytes(body)
	code, err := buf.ReadInt16()
	if err != nil {
		return 0, DelegationToken{}, err
	}
	var t DelegationToken
	read := []struct {
		dst *string
	}{
		{&t.Owner.Type}, {&t.Owner.Name}, {&t.TokenRequester.Type}, {&t.TokenRequester.Name},
	}
	for _, r := range read {
		s, err := buf.ReadCompactString()
		if err != nil {
			return code, DelegationToken{}, err
		}
		*r.dst = s
	}
	for _, dst := range []*int64{&t.IssueTimestamp, &t.ExpiryTimestamp, &t.MaxTimestamp} {
		v, err := buf.ReadInt64()
		if err != nil {
			return code, DelegationToken{}, err
		}
		*dst = v
	}
	if t.TokenID, err = buf.ReadCompactString(); err != nil {
		return code, DelegationToken{}, err
	}
	if t.HMAC, err = buf.ReadCompactBytes(); err != nil {
		return code, DelegationToken{}, err
	}
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return code, DelegationToken{}, err
	}
	return code, t, nil
}

// EncodeRenewDelegationTokenRequest encodes API 39 (flexible v2).
func EncodeRenewDelegationTokenRequest(hmac []byte, renewPeriodMs int64) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteCompactBytes(hmac)
	buf.WriteInt64(renewPeriodMs)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// EncodeExpireDelegationTokenRequest encodes API 40 (flexible v2).
func EncodeExpireDelegationTokenRequest(hmac []byte, expiryTimePeriodMs int64) []byte {
	buf := wire.NewBuffer(32)
	buf.WriteCompactBytes(hmac)
	buf.WriteInt64(expiryTimePeriodMs)
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeRenewOrExpireDelegationTokenResponse decodes the API 39/40 response
// (flexible): error_code, expiry_timestamp_ms, throttle_time_ms.
func DecodeRenewOrExpireDelegationTokenResponse(body []byte) (int16, int64, error) {
	buf := wire.FromBytes(body)
	code, err := buf.ReadInt16()
	if err != nil {
		return 0, 0, err
	}
	expiry, err := buf.ReadInt64()
	if err != nil {
		return code, 0, err
	}
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return code, expiry, err
	}
	return code, expiry, nil
}

// EncodeDescribeDelegationTokenRequest encodes API 41 (flexible v3). Nil owners
// describes all tokens the caller may see.
func EncodeDescribeDelegationTokenRequest(owners []Principal) []byte {
	buf := wire.NewBuffer(32)
	if owners == nil {
		buf.WriteUvarint(0) // null owners
	} else {
		buf.WriteCompactArrayLen(len(owners))
		for _, o := range owners {
			buf.WriteCompactString(o.Type)
			buf.WriteCompactString(o.Name)
			buf.WriteEmptyTagSection()
		}
	}
	buf.WriteEmptyTagSection()
	return buf.Bytes()
}

// DecodeDescribeDelegationTokenResponse decodes API 41 response (flexible v3).
func DecodeDescribeDelegationTokenResponse(body []byte) (int16, []DelegationToken, error) {
	buf := wire.FromBytes(body)
	code, err := buf.ReadInt16()
	if err != nil {
		return 0, nil, err
	}
	n, err := buf.ReadUvarint()
	if err != nil {
		return code, nil, err
	}
	var out []DelegationToken
	for i := 1; i < int(n); i++ {
		var t DelegationToken
		for _, dst := range []*string{&t.Owner.Type, &t.Owner.Name, &t.TokenRequester.Type, &t.TokenRequester.Name} {
			s, err := buf.ReadCompactString()
			if err != nil {
				return code, nil, err
			}
			*dst = s
		}
		for _, dst := range []*int64{&t.IssueTimestamp, &t.ExpiryTimestamp, &t.MaxTimestamp} {
			v, err := buf.ReadInt64()
			if err != nil {
				return code, nil, err
			}
			*dst = v
		}
		if t.TokenID, err = buf.ReadCompactString(); err != nil {
			return code, nil, err
		}
		if t.HMAC, err = buf.ReadCompactBytes(); err != nil {
			return code, nil, err
		}
		nr, err := buf.ReadUvarint()
		if err != nil {
			return code, nil, err
		}
		for j := 1; j < int(nr); j++ {
			var p Principal
			if p.Type, err = buf.ReadCompactString(); err != nil {
				return code, nil, err
			}
			if p.Name, err = buf.ReadCompactString(); err != nil {
				return code, nil, err
			}
			if err := buf.SkipTagSection(); err != nil {
				return code, nil, err
			}
			t.Renewers = append(t.Renewers, p)
		}
		if err := buf.SkipTagSection(); err != nil {
			return code, nil, err
		}
		out = append(out, t)
	}
	if _, err := buf.ReadInt32(); err != nil { // throttle_time_ms
		return code, out, err
	}
	return code, out, nil
}
