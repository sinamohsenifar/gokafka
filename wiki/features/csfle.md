---
title: Client-side field-level encryption (CSFLE)
type: feature
tags: [gokafka, csfle, encryption, schema-registry]
updated: 2026-06-30
---

# Client-side field-level encryption (CSFLE)

`schema.FieldEncrypter` encrypts/decrypts selected fields of a record map in place using **envelope encryption** — a fresh AES-256-GCM data key per call, wrapped by a `schema.KMS`. Pure stdlib crypto (`crypto/aes`, `crypto/cipher`, `crypto/rand`).

```go
kms, _ := schema.NewLocalKMS(masterKey)
enc := schema.NewFieldEncrypter(kms, "ssn", "email")
enc.EncryptFields(record) // before serialize
enc.DecryptFields(record) // after deserialize
```

- Encrypted field value: `"csfle:" + base64(varint(wlen) | wrappedDEK | nonce | ciphertext)`.
- GCM authentication → tampering or wrong key fails loudly.
- Built-in `LocalKMS`; the **`KMS` interface** lets callers plug AWS/GCP/Azure/Vault drivers themselves, keeping core dependency-free ([[decisions/adr-stdlib-only]]).

Among the Go clients only [[competitors/confluent-kafka-go|confluent-kafka-go]] offers CSFLE, and that needs cloud SDKs — this is the pure-Go alternative.

## Related
- [[packages/schema-registry]] · [[decisions/adr-stdlib-only]]
