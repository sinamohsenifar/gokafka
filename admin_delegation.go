package gokafka

import (
	"context"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// Principal is a Kafka principal (type + name), e.g. {"User", "alice"}.
type Principal struct {
	Type string
	Name string
}

// DelegationToken is a delegation token issued by the broker (KIP-48). HMAC is
// the shared secret used as the SASL/SCRAM password when authenticating with the
// token; treat it as a credential.
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

// CreateDelegationToken requests a delegation token (KIP-48, API 38). owner may
// be nil (the authenticated principal owns the token); renewers are principals
// allowed to renew it; maxLifetimeMs <= 0 uses the broker default. Requires the
// broker to have a delegation-token master key configured.
func (a *Admin) CreateDelegationToken(ctx context.Context, owner *Principal, renewers []Principal, maxLifetimeMs int64) (DelegationToken, error) {
	body := protocol.EncodeCreateDelegationTokenRequest(toProtoPrincipalPtr(owner), toProtoPrincipals(renewers), maxLifetimeMs)
	resp, err := a.requestAny(ctx, protocol.APICreateDelegationToken, protocol.VerCreateDelegationToken, body)
	if err != nil {
		return DelegationToken{}, err
	}
	code, tok, err := protocol.DecodeCreateDelegationTokenResponse(resp)
	if err != nil {
		return DelegationToken{}, err
	}
	if code != 0 {
		return DelegationToken{}, newKafkaError(code, "", 0, "create delegation token failed")
	}
	return fromProtoToken(tok), nil
}

// RenewDelegationToken extends a token's lifetime by renewPeriodMs and returns
// the new expiry timestamp (API 39).
func (a *Admin) RenewDelegationToken(ctx context.Context, hmac []byte, renewPeriodMs int64) (expiryTimestampMs int64, err error) {
	body := protocol.EncodeRenewDelegationTokenRequest(hmac, renewPeriodMs)
	resp, err := a.requestAny(ctx, protocol.APIRenewDelegationToken, protocol.VerRenewDelegationToken, body)
	if err != nil {
		return 0, err
	}
	code, expiry, err := protocol.DecodeRenewOrExpireDelegationTokenResponse(resp)
	if err != nil {
		return 0, err
	}
	if code != 0 {
		return 0, newKafkaError(code, "", 0, "renew delegation token failed")
	}
	return expiry, nil
}

// ExpireDelegationToken changes a token's expiry by expiryTimePeriodMs (negative
// to invalidate it now) and returns the new expiry timestamp (API 40).
func (a *Admin) ExpireDelegationToken(ctx context.Context, hmac []byte, expiryTimePeriodMs int64) (expiryTimestampMs int64, err error) {
	body := protocol.EncodeExpireDelegationTokenRequest(hmac, expiryTimePeriodMs)
	resp, err := a.requestAny(ctx, protocol.APIExpireDelegationToken, protocol.VerExpireDelegationToken, body)
	if err != nil {
		return 0, err
	}
	code, expiry, err := protocol.DecodeRenewOrExpireDelegationTokenResponse(resp)
	if err != nil {
		return 0, err
	}
	if code != 0 {
		return 0, newKafkaError(code, "", 0, "expire delegation token failed")
	}
	return expiry, nil
}

// DescribeDelegationTokens lists delegation tokens (API 41). With no owners it
// describes all tokens the caller is allowed to see.
func (a *Admin) DescribeDelegationTokens(ctx context.Context, owners ...Principal) ([]DelegationToken, error) {
	var protoOwners []protocol.Principal
	if len(owners) > 0 {
		protoOwners = toProtoPrincipals(owners)
	}
	body := protocol.EncodeDescribeDelegationTokenRequest(protoOwners)
	resp, err := a.requestAny(ctx, protocol.APIDescribeDelegationToken, protocol.VerDescribeDelegationToken, body)
	if err != nil {
		return nil, err
	}
	code, toks, err := protocol.DecodeDescribeDelegationTokenResponse(resp)
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, newKafkaError(code, "", 0, "describe delegation tokens failed")
	}
	out := make([]DelegationToken, 0, len(toks))
	for _, t := range toks {
		out = append(out, fromProtoToken(t))
	}
	return out, nil
}

func toProtoPrincipalPtr(p *Principal) *protocol.Principal {
	if p == nil {
		return nil
	}
	return &protocol.Principal{Type: p.Type, Name: p.Name}
}

func toProtoPrincipals(ps []Principal) []protocol.Principal {
	out := make([]protocol.Principal, len(ps))
	for i, p := range ps {
		out[i] = protocol.Principal{Type: p.Type, Name: p.Name}
	}
	return out
}

func fromProtoToken(t protocol.DelegationToken) DelegationToken {
	renewers := make([]Principal, len(t.Renewers))
	for i, r := range t.Renewers {
		renewers[i] = Principal{Type: r.Type, Name: r.Name}
	}
	return DelegationToken{
		Owner:           Principal{Type: t.Owner.Type, Name: t.Owner.Name},
		TokenRequester:  Principal{Type: t.TokenRequester.Type, Name: t.TokenRequester.Name},
		IssueTimestamp:  t.IssueTimestamp,
		ExpiryTimestamp: t.ExpiryTimestamp,
		MaxTimestamp:    t.MaxTimestamp,
		TokenID:         t.TokenID,
		HMAC:            t.HMAC,
		Renewers:        renewers,
	}
}
