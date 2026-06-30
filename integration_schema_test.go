//go:build integration

package gokafka_test

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka"
	"github.com/sinamohsenifar/gokafka/schema"
)

func TestIntegrationSchemaRegistry(t *testing.T) {
	url := os.Getenv("SCHEMA_REGISTRY_URL")
	if url == "" {
		url = "http://127.0.0.1:8081/apis/ccompat/v6"
	}
	_ = integrationBrokers(t) // skip if kafka env not configured

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reg, err := schema.New(schema.Config{URL: url})
	if err != nil {
		t.Fatal(err)
	}

	subject := "gokafka-it-" + time.Now().Format("150405.000")
	schemaJSON := `{"type":"object","properties":{"msg":{"type":"string"}}}`
	id, err := reg.RegisterJSON(ctx, subject, schemaJSON)
	if err != nil {
		t.Fatal(err)
	}
	if id <= 0 {
		t.Fatalf("schema id=%d", id)
	}

	raw, err := gokafka.JSONPayload{V: map[string]string{"msg": "hello"}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	payload := gokafka.EncodeSchemaWire(id, raw)
	gotID, raw, err := gokafka.DecodeSchemaWire(payload)
	if err != nil {
		t.Fatal(err)
	}
	if gotID != id {
		t.Fatalf("id=%d want %d", gotID, id)
	}
	if string(raw) == "" {
		t.Fatal("empty decoded payload")
	}
}

func TestIntegrationSchemaAvroRoundTrip(t *testing.T) {
	url := os.Getenv("SCHEMA_REGISTRY_URL")
	if url == "" {
		url = "http://127.0.0.1:8081/apis/ccompat/v6"
	}
	_ = integrationBrokers(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reg, err := schema.New(schema.Config{URL: url})
	if err != nil {
		t.Fatal(err)
	}

	subject := "gokafka-avro-" + time.Now().Format("150405.000")
	schemaText := `{"type":"record","name":"Event","fields":[{"name":"msg","type":"string"}]}`
	serde := schema.NewSerde(reg, schema.SerdeConfig{Subject: subject, Format: schema.FormatAvro})

	encoded, err := serde.EncodeAvro(ctx, schemaText, map[string]any{"msg": "avro-it"})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := serde.DecodeAvro(ctx, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded["msg"] != "avro-it" {
		t.Fatalf("msg=%v", decoded["msg"])
	}
}

// TestIntegrationSchemaProtobufRoundTrip verifies the Protobuf path end-to-end
// against a real registry: a .proto schema is registered (schemaType=PROTOBUF),
// pre-encoded Protobuf bytes are wrapped with Confluent framing (schema id +
// message indexes), and the framing decodes back to the same indexes + payload.
// GoKafka owns the wire/registry layers; the message bytes are the app's (via
// google.golang.org/protobuf), so a hand-encoded message keeps this stdlib-only.
func TestIntegrationSchemaProtobufRoundTrip(t *testing.T) {
	url := os.Getenv("SCHEMA_REGISTRY_URL")
	if url == "" {
		url = "http://127.0.0.1:8081/apis/ccompat/v6"
	}
	_ = integrationBrokers(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reg, err := schema.New(schema.Config{URL: url})
	if err != nil {
		t.Fatal(err)
	}

	subject := "gokafka-pb-" + time.Now().Format("150405.000")
	protoText := "syntax = \"proto3\";\nmessage Greeting { string text = 1; }\n"
	serde := schema.NewSerde(reg, schema.SerdeConfig{Subject: subject, Format: schema.FormatProtobuf})

	// Greeting{text:"hello"} encoded as Protobuf: field 1, wire type 2 (LEN),
	// length 5, "hello".
	protoPayload := []byte{0x0A, 0x05, 'h', 'e', 'l', 'l', 'o'}
	encoded, err := serde.EncodeProtobuf(ctx, protoText, protoPayload)
	if err != nil {
		t.Fatal(err)
	}
	if serde.SchemaID() <= 0 {
		t.Fatalf("protobuf schema was not registered: id=%d", serde.SchemaID())
	}
	indexes, payload, err := serde.DecodeProtobuf(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(indexes) != 1 || indexes[0] != 0 {
		t.Fatalf("message indexes=%v want [0]", indexes)
	}
	if !bytes.Equal(payload, protoPayload) {
		t.Fatalf("payload=% x want % x", payload, protoPayload)
	}
}
