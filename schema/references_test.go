package schema_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sinamohsenifar/gokafka/schema"
)

// TestRegisterWithReferencesSendsBody proves the client actually puts the
// references array (and schemaType) on the wire — not just accepts the argument.
func TestRegisterWithReferencesSendsBody(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":7}`))
	}))
	defer srv.Close()

	reg, err := schema.New(schema.Config{URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	refs := []schema.Reference{{Name: "base.proto", Subject: "base-value", Version: 3}}
	id, err := reg.RegisterWithReferences(context.Background(), "dep-value", "PROTOBUF",
		`syntax = "proto3"; import "base.proto"; message Dep { Base b = 1; }`, refs)
	if err != nil {
		t.Fatal(err)
	}
	if id != 7 {
		t.Fatalf("id=%d want 7", id)
	}
	if gotBody["schemaType"] != "PROTOBUF" {
		t.Fatalf("schemaType=%v", gotBody["schemaType"])
	}
	rawRefs, ok := gotBody["references"].([]any)
	if !ok || len(rawRefs) != 1 {
		t.Fatalf("references not sent on the wire: %v", gotBody["references"])
	}
	r0 := rawRefs[0].(map[string]any)
	if r0["name"] != "base.proto" || r0["subject"] != "base-value" || r0["version"].(float64) != 3 {
		t.Fatalf("reference body wrong: %v", r0)
	}
}

// TestRegisterWithoutReferencesOmitsField confirms the plain register path does
// not emit an empty references array.
func TestRegisterWithoutReferencesOmitsField(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, _ = w.Write([]byte(`{"id":1}`))
	}))
	defer srv.Close()

	reg, _ := schema.New(schema.Config{URL: srv.URL})
	if _, err := reg.RegisterProtobuf(context.Background(), "s", `syntax = "proto3"; message M {}`); err != nil {
		t.Fatal(err)
	}
	if _, ok := gotBody["references"]; ok {
		t.Fatalf("plain register should omit references, body=%v", gotBody)
	}
}

// TestDecodeProtobufSchemaIDGuard proves DecodeProtobuf applies the configured
// wire-schema-id guard (parity with DecodeAvro), not just strips framing.
func TestDecodeProtobufSchemaIDGuard(t *testing.T) {
	reg := schema.NewMockRegistry()
	serde := schema.NewSerde(reg, schema.SerdeConfig{
		Subject: "s", Format: schema.FormatProtobuf, ExpectedSchemaID: 999,
	})
	enc, err := serde.EncodeProtobuf(context.Background(), `syntax = "proto3"; message M { string a = 1; }`, []byte{0x0A, 0x01, 'x'})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := serde.DecodeProtobuf(enc); err == nil {
		t.Fatal("expected schema-id guard to reject a wire id that differs from ExpectedSchemaID")
	}

	// With the guard satisfied, decode returns the original payload and indexes.
	ok := schema.NewSerde(reg, schema.SerdeConfig{Subject: "s", Format: schema.FormatProtobuf, ExpectedSchemaID: serde.SchemaID()})
	idx, payload, err := ok.DecodeProtobuf(enc)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx) != 1 || idx[0] != 0 {
		t.Fatalf("indexes=%v want [0]", idx)
	}
	if string(payload) != "\x0a\x01x" {
		t.Fatalf("payload=% x", payload)
	}
}
