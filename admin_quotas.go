package gokafka

import (
	"context"

	"github.com/sinamohsenifar/gokafka/internal/protocol"
)

// Quota entity type names (KIP-546).
const (
	QuotaEntityUser     = "user"
	QuotaEntityClientID = "client-id"
	QuotaEntityIP       = "ip"
)

// QuotaEntity identifies a quota target as entity-type -> entity-name. An empty
// name ("") means the default entity for that type.
type QuotaEntity map[string]string

// QuotaEntry is a described quota entity and its configured values.
type QuotaEntry struct {
	Entity QuotaEntity
	Values map[string]float64
}

// QuotaFilterComponent is one clause of a DescribeClientQuotas filter.
type QuotaFilterComponent struct {
	EntityType string
	// MatchName, when non-nil, restricts to that exact entity name. Nil matches
	// any value of EntityType (set MatchDefault to instead match the default).
	MatchName    *string
	MatchDefault bool
}

// DescribeClientQuotas returns quota entries matching the filter. strict=true
// excludes entities that have components not listed in the filter.
func (a *Admin) DescribeClientQuotas(ctx context.Context, filter []QuotaFilterComponent, strict bool) ([]QuotaEntry, error) {
	comps := make([]protocol.QuotaComponent, len(filter))
	for i, f := range filter {
		c := protocol.QuotaComponent{EntityType: f.EntityType, MatchType: protocol.QuotaMatchAny}
		switch {
		case f.MatchDefault:
			c.MatchType = protocol.QuotaMatchDefault
		case f.MatchName != nil:
			c.MatchType = protocol.QuotaMatchExact
			c.Match = f.MatchName
		}
		comps[i] = c
	}
	ver := a.client.cluster.NegotiatedVersion(protocol.APIDescribeClientQuotas, protocol.VerDescribeClientQuotas)
	if ver < 0 {
		ver = protocol.VerDescribeClientQuotas
	}
	body := protocol.EncodeDescribeClientQuotasRequest(ver, comps, strict)
	resp, err := a.requestAny(ctx, protocol.APIDescribeClientQuotas, ver, body)
	if err != nil {
		return nil, err
	}
	code, msg, entries, err := protocol.DecodeDescribeClientQuotasResponse(ver, resp)
	if err != nil {
		return nil, err
	}
	if code != 0 {
		return nil, newKafkaError(code, "", 0, msg)
	}
	out := make([]QuotaEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, QuotaEntry{Entity: entityToMap(e.Entity), Values: e.Values})
	}
	return out, nil
}

// QuotaOp sets or removes a single quota key for an entity.
type QuotaOp struct {
	Key    string
	Value  float64
	Remove bool
}

// SetClientQuota sets or removes quota values for one entity. Pass an op with
// Remove=true to delete a key. Common keys: "producer_byte_rate",
// "consumer_byte_rate", "request_percentage".
func (a *Admin) SetClientQuota(ctx context.Context, entity QuotaEntity, ops ...QuotaOp) error {
	if len(ops) == 0 {
		return nil
	}
	alt := protocol.QuotaAlteration{Entity: mapToEntity(entity)}
	for _, o := range ops {
		alt.Ops = append(alt.Ops, protocol.QuotaOp{Key: o.Key, Value: o.Value, Remove: o.Remove})
	}
	ver := a.client.cluster.NegotiatedVersion(protocol.APIAlterClientQuotas, protocol.VerAlterClientQuotas)
	if ver < 0 {
		ver = protocol.VerAlterClientQuotas
	}
	body := protocol.EncodeAlterClientQuotasRequest(ver, []protocol.QuotaAlteration{alt}, false)
	resp, err := a.requestAny(ctx, protocol.APIAlterClientQuotas, ver, body)
	if err != nil {
		return err
	}
	results, err := protocol.DecodeAlterClientQuotasResponse(ver, resp)
	if err != nil {
		return err
	}
	for _, r := range results {
		if r.ErrorCode != 0 {
			return newKafkaError(r.ErrorCode, "", 0, r.ErrorMessage)
		}
	}
	return nil
}

func entityToMap(elems []protocol.QuotaEntityElem) QuotaEntity {
	m := make(QuotaEntity, len(elems))
	for _, e := range elems {
		if e.Name == nil {
			m[e.Type] = "" // default entity
		} else {
			m[e.Type] = *e.Name
		}
	}
	return m
}

func mapToEntity(m QuotaEntity) []protocol.QuotaEntityElem {
	out := make([]protocol.QuotaEntityElem, 0, len(m))
	for t, name := range m {
		el := protocol.QuotaEntityElem{Type: t}
		if name != "" {
			n := name
			el.Name = &n
		}
		out = append(out, el)
	}
	return out
}
