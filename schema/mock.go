package schema

import (
	"context"
	"fmt"
	"sync"
)

// MockRegistry is an in-memory Schema Registry for tests. It implements
// SchemaClient (register + fetch-by-id), so Serde encode/decode round-trips can
// be exercised without a running registry — equivalent to confluent-kafka-go's
// mock schema-registry client. Identical schema text under the same subject is
// deduplicated to one id, mirroring a real registry. Safe for concurrent use.
type MockRegistry struct {
	mu       sync.Mutex
	nextID   int
	byID     map[int]string   // schema id -> schema text
	idOf     map[string]int   // subject\x00schema -> id (dedup per subject)
	versions map[string][]int // subject -> ordered schema ids (versions)
	types    map[int]string   // schema id -> schema type (AVRO/JSON/PROTOBUF)
}

// NewMockRegistry creates an empty in-memory registry.
func NewMockRegistry() *MockRegistry {
	return &MockRegistry{
		nextID:   0,
		byID:     map[int]string{},
		idOf:     map[string]int{},
		versions: map[string][]int{},
		types:    map[int]string{},
	}
}

func (m *MockRegistry) register(subject, schemaType, schema string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := subject + "\x00" + schema
	if id, ok := m.idOf[key]; ok {
		return id, nil
	}
	m.nextID++
	id := m.nextID
	m.byID[id] = schema
	m.idOf[key] = id
	m.types[id] = schemaType
	m.versions[subject] = append(m.versions[subject], id)
	return id, nil
}

// RegisterAvro registers an Avro schema under a subject.
func (m *MockRegistry) RegisterAvro(_ context.Context, subject, schema string) (int, error) {
	return m.register(subject, "AVRO", schema)
}

// RegisterJSON registers a JSON schema under a subject.
func (m *MockRegistry) RegisterJSON(_ context.Context, subject, schema string) (int, error) {
	return m.register(subject, "JSON", schema)
}

// RegisterJSONSchema registers a JSON Schema document under a subject.
func (m *MockRegistry) RegisterJSONSchema(_ context.Context, subject, schema string) (int, error) {
	return m.register(subject, "JSON", schema)
}

// RegisterProtobuf registers a Protobuf schema under a subject.
func (m *MockRegistry) RegisterProtobuf(_ context.Context, subject, schema string) (int, error) {
	return m.register(subject, "PROTOBUF", schema)
}

// RegisterWithReferences registers a schema of the given type with references.
// The in-memory mock accepts and stores the schema like any other (it does not
// resolve imports), so serde round-trips that pass References still work offline.
func (m *MockRegistry) RegisterWithReferences(_ context.Context, subject, schemaType, schema string, _ []Reference) (int, error) {
	return m.register(subject, schemaType, schema)
}

// SchemaByID returns the schema text for an id, or an error if unknown.
func (m *MockRegistry) SchemaByID(_ context.Context, id int) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.byID[id]
	if !ok {
		return "", fmt.Errorf("schema: mock registry has no schema id %d", id)
	}
	return s, nil
}

// ListVersions returns the registered schema ids for a subject, in registration order.
func (m *MockRegistry) ListVersions(subject string) []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int(nil), m.versions[subject]...)
}
