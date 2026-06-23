# GSSAPI / Kerberos SASL

## Status

**SPNEGO pass-through implemented** (v0.21.0). GoKafka performs multi-round SASL GSSAPI when you supply an initial token and/or a `TokenProvider`. Full KDC/keytab parsing remains out of scope for stdlib-only builds.

## Why full Kerberos is hard in pure Go

Kafka GSSAPI SASL uses **SPNEGO** over SASL, which typically requires:

- ASN.1 DER encoding of Kerberos tickets (GSS-API tokens)
- Integration with a KDC (MIT Kerberos, Active Directory, etc.)
- Channel binding considerations for TLS + SASL_SSL

The Go standard library provides `crypto` primitives but not a full Kerberos/GSS-API stack.

## Supported pattern (v0.21+)

Use an external Kerberos stack (`kinit`, MIT krb5, Active Directory tools) to obtain SPNEGO tokens, then wire them through GoKafka:

```go
gokafka.WithSecurity(gokafka.SecurityConfig{
    Protocol: gokafka.SecuritySASLSSL,
    SASL: gokafka.SASLConfig{
        Mechanism: gokafka.SASLGSSAPI,
        Kerberos: gokafka.KerberosConfig{
            Principal: "kafka/client@REALM",
            InitToken: firstToken, // optional first outbound token
            TokenProvider: func(ctx context.Context, challenge []byte) ([]byte, error) {
                return yourKrb5Exchange(ctx, challenge)
            },
        },
    },
})
```

## Alternatives used in production

| Approach | Notes |
|----------|-------|
| **franz-go** / **sarama** | Mature GSSAPI via build tags or cgo |
| **confluent-kafka-go** | librdkafka handles SPNEGO |
| **OAuthBearer** | Supported in GoKafka wire; use for managed Kafka (Azure Event Hubs, AWS MSK IAM patterns vary) |

## Roadmap

1. OAuthBearer integration test (docker JAAS) — **v0.20** ✅
2. SPNEGO token pass-through — **v0.21** ✅
3. Full KDC/keytab integration — **out of scope** for stdlib-only v1; optional build tag in a future major version
