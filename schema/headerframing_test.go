package schema

import (
	"context"
	"fmt"
	"testing"
)

func TestHeaderFramedRoundTrip(t *testing.T) {
	ctx := context.Background()
	reg := NewMockRegistry()
	s := NewSerde(reg, SerdeConfig{Subject: "users-value", Format: FormatAvro})
	schema := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`

	payload, hdr, err := s.EncodeAvroHeaderFramed(ctx, schema, map[string]any{"id": int32(9), "name": "bo"})
	if err != nil {
		t.Fatal(err)
	}
	// Header is 5 bytes (magic + id); payload carries no prefix (first byte is
	// Avro data, not the 0x00 magic in the general case).
	if len(hdr) != 5 || hdr[0] != 0 {
		t.Fatalf("header value = %v", hdr)
	}
	got, err := s.DecodeAvroHeaderFramed(ctx, payload, hdr)
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got["id"]) != "9" || fmt.Sprint(got["name"]) != "bo" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestSchemaIDHeaderKey(t *testing.T) {
	if SchemaIDHeaderKey(false) != "__value_schema_id" || SchemaIDHeaderKey(true) != "__key_schema_id" {
		t.Fatal("unexpected header keys")
	}
}
