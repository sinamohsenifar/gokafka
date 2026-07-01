// Command csfle demonstrates client-side field-level encryption (CSFLE) with
// GoKafka's schema package. Selected PII fields of a record are encrypted in the
// client BEFORE the record is serialized and produced, and decrypted AFTER it is
// consumed and deserialized — so the broker only ever sees ciphertext for those
// fields. This runs entirely offline: no broker or network is required.
//
// How it works (envelope encryption):
//   - A KMS holds a long-lived key-encryption key (KEK) / master key.
//   - For each EncryptFields call a fresh AES-256-GCM data encryption key (DEK)
//     is generated, used to encrypt the chosen field values, then itself wrapped
//     (encrypted) by the KMS. The wrapped DEK travels alongside the ciphertext.
//   - To decrypt, the KMS unwraps the DEK, which then decrypts the fields.
//
// GoKafka ships a pure-Go schema.LocalKMS plus a small schema.KMS interface.
// Cloud KMS drivers (AWS KMS, GCP KMS, Azure Key Vault, HashiCorp Vault) plug in
// by implementing that interface — WrapDEK/UnwrapDEK — which keeps GoKafka's core
// dependency-free and standard-library only. Only your KMS driver pulls in a SDK.
package main

import (
	"encoding/json"
	"log"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/schema"
)

func main() {
	log.Printf("gokafka version %s", gokafka.VersionString())

	// The master key (KEK) is the root of trust. In production this lives in a
	// real KMS and never leaves it; here we use a fixed 32-byte key so the demo
	// is reproducible. 16-, 24-, or 32-byte keys select AES-128/192/256-GCM.
	masterKey := []byte("0123456789abcdef0123456789abcdef") // 32 bytes -> AES-256
	kms, err := schema.NewLocalKMS(masterKey)
	if err != nil {
		log.Fatal(err)
	}

	// Declare which fields carry PII and must be encrypted. Non-listed fields
	// (here "id" and "plan") stay in clear text, so they remain queryable and
	// usable for partitioning while the sensitive ones are protected.
	enc := schema.NewFieldEncrypter(kms, "email", "ssn")

	// A record modeled as a map[string]any — the shape a JSON/Avro Serde would
	// hand you. EncryptFields/DecryptFields operate on this map in place.
	record := map[string]any{
		"id":    "user-42",
		"email": "alice@example.com",
		"ssn":   "123-45-6789",
		"plan":  "enterprise",
	}
	log.Printf("plaintext:  %s", mustJSON(record))

	// Encrypt the PII fields in place. Each selected value is JSON-encoded, then
	// sealed with the per-record DEK; the result is a "csfle:"-prefixed, base64
	// envelope string. This is what you'd serialize and hand to the producer —
	// the broker stores only ciphertext for "email" and "ssn".
	if err := enc.EncryptFields(record); err != nil {
		log.Fatalf("encrypt: %v", err)
	}
	log.Printf("encrypted:  %s", mustJSON(record))

	// Decrypt in place — the step a consumer runs after deserializing. The KMS
	// unwraps the DEK, the DEK decrypts the fields, and values round-trip back to
	// their original types (strings stay strings, numbers stay JSON numbers).
	if err := enc.DecryptFields(record); err != nil {
		log.Fatalf("decrypt: %v", err)
	}
	log.Printf("decrypted:  %s", mustJSON(record))

	// EncryptFields skips fields that are absent or already encrypted, and
	// DecryptFields skips fields that are absent or not marked encrypted, so both
	// are safe to call idempotently on partially-processed records.
}

// mustJSON renders a record map for display; encryption never touches the keys,
// only the selected values, so the structure stays readable throughout.
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		log.Fatal(err)
	}
	return string(b)
}
