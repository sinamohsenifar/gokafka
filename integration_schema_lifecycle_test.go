//go:build integration

package gokafka_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sinamohsenifar/gokafka/schema"
)

// TestIntegrationSchemaLifecycle exercises the Schema Registry lifecycle
// endpoints (register, list, get-by-version, compatibility check, config
// get/set, delete) against the running registry.
func TestIntegrationSchemaLifecycle(t *testing.T) {
	url := os.Getenv("SCHEMA_REGISTRY_URL")
	if url == "" {
		url = "http://127.0.0.1:8081/apis/ccompat/v6"
	}
	_ = integrationBrokers(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	reg, err := schema.New(schema.Config{URL: url})
	if err != nil {
		t.Fatal(err)
	}

	subject := schema.SubjectForTopic(fmt.Sprintf("gokafka-life-%d", time.Now().UnixNano()), false)
	v1 := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"}]}`
	// Backward-compatible evolution: add a field with a default.
	v2 := `{"type":"record","name":"User","fields":[{"name":"id","type":"int"},{"name":"name","type":"string","default":""}]}`
	// Incompatible: change id's type.
	bad := `{"type":"record","name":"User","fields":[{"name":"id","type":"string"}]}`

	id1, err := reg.RegisterAvro(ctx, subject, v1)
	if err != nil {
		t.Fatalf("register v1: %v", err)
	}
	t.Cleanup(func() { _, _ = reg.DeleteSubject(context.Background(), subject, true) })

	// Ensure BACKWARD so the compatibility assertions are deterministic.
	if err := reg.SetCompatibility(ctx, subject, "BACKWARD"); err != nil {
		t.Fatalf("set compatibility: %v", err)
	}
	if lvl, err := reg.Compatibility(ctx, subject); err != nil || lvl != "BACKWARD" {
		t.Fatalf("compatibility = %q err=%v, want BACKWARD", lvl, err)
	}

	okCompat, err := reg.IsCompatible(ctx, subject, "latest", "AVRO", v2)
	if err != nil || !okCompat {
		t.Fatalf("v2 should be backward-compatible: ok=%v err=%v", okCompat, err)
	}
	badCompat, err := reg.IsCompatible(ctx, subject, "latest", "AVRO", bad)
	if err != nil {
		t.Fatalf("compat check (bad): %v", err)
	}
	if badCompat {
		t.Fatalf("type change must be incompatible under BACKWARD")
	}

	id2, err := reg.RegisterAvro(ctx, subject, v2)
	if err != nil {
		t.Fatalf("register v2: %v", err)
	}
	if id2 == id1 {
		t.Fatalf("v2 should get a new schema id (got %d == %d)", id2, id1)
	}

	versions, err := reg.ListVersions(ctx, subject)
	if err != nil || len(versions) != 2 {
		t.Fatalf("ListVersions = %v err=%v, want 2 versions", versions, err)
	}
	latest, err := reg.SchemaByVersion(ctx, subject, "latest")
	if err != nil || latest.ID != id2 {
		t.Fatalf("SchemaByVersion latest = %+v err=%v, want id %d", latest, err, id2)
	}

	subjects, err := reg.ListSubjects(ctx)
	if err != nil {
		t.Fatalf("ListSubjects: %v", err)
	}
	found := false
	for _, s := range subjects {
		if s == subject {
			found = true
		}
	}
	if !found {
		t.Fatalf("subject %q not in ListSubjects", subject)
	}

	// IsRegistered: v1 is registered under the subject; an unrelated schema is not.
	sv, ok, err := reg.IsRegistered(ctx, subject, "AVRO", v1)
	if err != nil || !ok || sv.ID != id1 {
		t.Fatalf("IsRegistered(v1) = (%+v, %v, %v), want found id %d", sv, ok, err, id1)
	}
	unrelated := `{"type":"record","name":"Other","fields":[{"name":"x","type":"long"}]}`
	if _, ok, err := reg.IsRegistered(ctx, subject, "AVRO", unrelated); err != nil || ok {
		t.Fatalf("IsRegistered(unrelated) = (ok=%v, err=%v), want not found", ok, err)
	}

	// Mode: the /mode endpoint is Confluent-specific; some compatibility layers
	// (e.g. Apicurio ccompat) return "Operation not supported". Treat that as a
	// skip — the global mode is READWRITE on a registry that does support it.
	if mode, err := reg.Mode(ctx, ""); err != nil {
		t.Logf("Mode(global) not supported by this registry: %v", err)
	} else if mode != "READWRITE" {
		t.Logf("global mode = %q (expected READWRITE on a default registry)", mode)
	}

	if _, err := reg.DeleteSubject(ctx, subject, false); err != nil {
		t.Fatalf("soft delete subject: %v", err)
	}
}
