package schema

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sinamohsenifar/gokafka/internal/limits"
)

// Config configures Schema Registry HTTP access.
type Config struct {
	URL      string
	Username string
	Password string
}

// Registry is a Confluent Schema Registry REST client (stdlib net/http).
type Registry struct {
	base   string
	client *http.Client
	user   string
	pass   string
}

func New(cfg Config) (*Registry, error) {
	if cfg.URL == "" {
		return nil, errors.New("schema: URL required")
	}
	base := strings.TrimRight(cfg.URL, "/")
	return &Registry{
		base:   base,
		client: &http.Client{Timeout: 15 * time.Second},
		user:   cfg.Username,
		pass:   cfg.Password,
	}, nil
}

// SchemaRegistryConfig is a public alias used by gokafka root package.
type SchemaRegistryConfig = Config

// EncodeWire prepends Confluent magic byte and schema ID to payload bytes.
func EncodeWire(schemaID int, payload []byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:5], uint32(schemaID))
	copy(out[5:], payload)
	return out
}

// DecodeWire strips Confluent wire header and returns schema ID + payload.
func DecodeWire(b []byte) (schemaID int, payload []byte, err error) {
	if len(b) < 5 {
		return 0, nil, errors.New("schema: wire format too short")
	}
	if b[0] != 0 {
		return 0, nil, errors.New("schema: unknown magic byte")
	}
	id := int(binary.BigEndian.Uint32(b[1:5]))
	return id, b[5:], nil
}

type schemaResponse struct {
	ID     int    `json:"id"`
	Schema string `json:"schema"`
}

// RegisterJSON registers a JSON schema and returns its ID.
func (r *Registry) RegisterJSON(ctx context.Context, subject, schema string) (int, error) {
	return r.register(ctx, subject, "JSON", schema)
}

// RegisterJSONSchema registers a JSON Schema document (alias for RegisterJSON with explicit type).
func (r *Registry) RegisterJSONSchema(ctx context.Context, subject, schema string) (int, error) {
	return r.register(ctx, subject, "JSON", schema)
}

// RegisterAvro registers an Avro schema.
func (r *Registry) RegisterAvro(ctx context.Context, subject, schema string) (int, error) {
	return r.register(ctx, subject, "AVRO", schema)
}

// RegisterProtobuf registers a Protobuf schema.
func (r *Registry) RegisterProtobuf(ctx context.Context, subject, schema string) (int, error) {
	return r.register(ctx, subject, "PROTOBUF", schema)
}

func (r *Registry) register(ctx context.Context, subject, schemaType, schema string) (int, error) {
	body := map[string]any{"schemaType": schemaType, "schema": schema}
	var resp schemaResponse
	path := "/subjects/" + escapeSubjectPath(subject) + "/versions"
	if err := r.post(ctx, path, body, &resp); err != nil {
		return 0, err
	}
	return resp.ID, nil
}

// SchemaByID fetches schema text by ID.
func (r *Registry) SchemaByID(ctx context.Context, id int) (string, error) {
	var resp schemaResponse
	path := fmt.Sprintf("/schemas/ids/%d", id)
	if err := r.get(ctx, path, &resp); err != nil {
		return "", err
	}
	return resp.Schema, nil
}

// SchemaByGUID fetches schema text by its globally-unique id (GUID), the
// content-addressed identifier newer Schema Registry versions expose alongside
// the numeric id (GET /schemas/guids/{guid}).
func (r *Registry) SchemaByGUID(ctx context.Context, guid string) (string, error) {
	var resp schemaResponse
	path := "/schemas/guids/" + guid
	if err := r.get(ctx, path, &resp); err != nil {
		return "", err
	}
	return resp.Schema, nil
}

// SubjectForTopic returns the default TopicNameStrategy subject for a topic:
// "<topic>-value" (or "<topic>-key" when isKey is true). One schema per topic.
func SubjectForTopic(topic string, isKey bool) string {
	if isKey {
		return topic + "-key"
	}
	return topic + "-value"
}

// SubjectForRecord returns the RecordNameStrategy subject: the fully-qualified
// record name itself (e.g. "com.example.User"). This lets multiple event types
// share a topic, with compatibility scoped per record type rather than per topic.
// Matches Confluent's RecordNameStrategy (no -key/-value suffix; the record name
// is the discriminator).
func SubjectForRecord(recordName string) string {
	return recordName
}

// SubjectForTopicRecord returns the TopicRecordNameStrategy subject:
// "<topic>-<recordName>" (e.g. "orders-com.example.User"). Multiple event types
// per topic, with compatibility scoped per topic-and-record. Matches Confluent's
// TopicRecordNameStrategy.
func SubjectForTopicRecord(topic, recordName string) string {
	return topic + "-" + recordName
}

// SubjectNameStrategy derives the registry subject for a (topic, recordName,
// isKey) tuple, so callers can plug TopicNameStrategy / RecordNameStrategy /
// TopicRecordNameStrategy (or a custom rule). recordName is the schema's
// fully-qualified name; it is ignored by TopicNameStrategy.
type SubjectNameStrategy func(topic, recordName string, isKey bool) string

// TopicNameStrategy is the default: "<topic>-key"/"<topic>-value".
func TopicNameStrategy(topic, _ string, isKey bool) string { return SubjectForTopic(topic, isKey) }

// RecordNameStrategy uses the fully-qualified record name as the subject.
func RecordNameStrategy(_ string, recordName string, _ bool) string {
	return SubjectForRecord(recordName)
}

// TopicRecordNameStrategy uses "<topic>-<recordName>" as the subject.
func TopicRecordNameStrategy(topic, recordName string, _ bool) string {
	return SubjectForTopicRecord(topic, recordName)
}

// SubjectVersion is a registered schema version under a subject.
type SubjectVersion struct {
	Subject    string `json:"subject"`
	ID         int    `json:"id"`
	Version    int    `json:"version"`
	Schema     string `json:"schema"`
	SchemaType string `json:"schemaType,omitempty"`
}

// ListSubjects returns all registered subjects.
func (r *Registry) ListSubjects(ctx context.Context) ([]string, error) {
	var out []string
	if err := r.get(ctx, "/subjects", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ListVersions returns the version numbers registered under a subject.
func (r *Registry) ListVersions(ctx context.Context, subject string) ([]int, error) {
	var out []int
	if err := r.get(ctx, "/subjects/"+escapeSubjectPath(subject)+"/versions", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// SchemaByVersion fetches a specific version under a subject. version may be a
// number or "latest" (the default when empty).
func (r *Registry) SchemaByVersion(ctx context.Context, subject, version string) (SubjectVersion, error) {
	if version == "" {
		version = "latest"
	}
	var out SubjectVersion
	path := "/subjects/" + escapeSubjectPath(subject) + "/versions/" + url.PathEscape(version)
	if err := r.get(ctx, path, &out); err != nil {
		return SubjectVersion{}, err
	}
	return out, nil
}

// IsCompatible reports whether schema is compatible under the subject's
// configured rule, checked against the given version ("latest" if empty).
func (r *Registry) IsCompatible(ctx context.Context, subject, version, schemaType, schema string) (bool, error) {
	if version == "" {
		version = "latest"
	}
	body := map[string]any{"schema": schema}
	if schemaType != "" {
		body["schemaType"] = schemaType
	}
	var out struct {
		IsCompatible bool `json:"is_compatible"`
	}
	path := "/compatibility/subjects/" + escapeSubjectPath(subject) + "/versions/" + url.PathEscape(version)
	if err := r.post(ctx, path, body, &out); err != nil {
		return false, err
	}
	return out.IsCompatible, nil
}

// Compatibility returns the effective compatibility level for a subject (pass
// subject "" for the global level): BACKWARD, FORWARD, FULL, NONE, or a
// _TRANSITIVE variant.
func (r *Registry) Compatibility(ctx context.Context, subject string) (string, error) {
	path := "/config"
	if subject != "" {
		path = "/config/" + escapeSubjectPath(subject) + "?defaultToGlobal=true"
	}
	var out struct {
		CompatibilityLevel string `json:"compatibilityLevel"`
		Compatibility      string `json:"compatibility"`
	}
	if err := r.get(ctx, path, &out); err != nil {
		return "", err
	}
	if out.CompatibilityLevel != "" {
		return out.CompatibilityLevel, nil
	}
	return out.Compatibility, nil
}

// SetCompatibility sets the compatibility level (pass subject "" for global).
func (r *Registry) SetCompatibility(ctx context.Context, subject, level string) error {
	path := "/config"
	if subject != "" {
		path = "/config/" + escapeSubjectPath(subject)
	}
	return r.put(ctx, path, map[string]any{"compatibility": level}, nil)
}

// Mode returns the registry mode (pass subject "" for the global mode):
// READWRITE, READONLY, or IMPORT.
func (r *Registry) Mode(ctx context.Context, subject string) (string, error) {
	path := "/mode"
	if subject != "" {
		path = "/mode/" + escapeSubjectPath(subject) + "?defaultToGlobal=true"
	}
	var out struct {
		Mode string `json:"mode"`
	}
	if err := r.get(ctx, path, &out); err != nil {
		return "", err
	}
	return out.Mode, nil
}

// SetMode sets the registry mode (pass subject "" for global): READWRITE,
// READONLY, or IMPORT.
func (r *Registry) SetMode(ctx context.Context, subject, mode string) error {
	path := "/mode"
	if subject != "" {
		path = "/mode/" + escapeSubjectPath(subject)
	}
	return r.put(ctx, path, map[string]any{"mode": mode}, nil)
}

// IsRegistered checks whether a schema is already registered under a subject
// without registering it (POST /subjects/{subject}). If found, it returns the
// existing subject/version/id and ok=true; if the schema or subject is not
// found, ok is false with a nil error.
func (r *Registry) IsRegistered(ctx context.Context, subject, schemaType, schema string) (SubjectVersion, bool, error) {
	body := map[string]any{"schema": schema}
	if schemaType != "" {
		body["schemaType"] = schemaType
	}
	b, err := json.Marshal(body)
	if err != nil {
		return SubjectVersion{}, false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.base+"/subjects/"+escapeSubjectPath(subject), bytes.NewReader(b))
	if err != nil {
		return SubjectVersion{}, false, err
	}
	var out SubjectVersion
	found, err := r.doLookup(req, &out)
	if err != nil || !found {
		return SubjectVersion{}, false, err
	}
	return out, true, nil
}

// DeleteSubjectVersion deletes one version (soft by default; permanent=true frees
// it, only allowed after a soft delete). Returns the deleted version number.
func (r *Registry) DeleteSubjectVersion(ctx context.Context, subject, version string, permanent bool) (int, error) {
	if version == "" {
		version = "latest"
	}
	path := "/subjects/" + escapeSubjectPath(subject) + "/versions/" + url.PathEscape(version)
	if permanent {
		path += "?permanent=true"
	}
	var out int
	if err := r.del(ctx, path, &out); err != nil {
		return 0, err
	}
	return out, nil
}

// DeleteSubject deletes a whole subject (soft by default). Returns deleted versions.
func (r *Registry) DeleteSubject(ctx context.Context, subject string, permanent bool) ([]int, error) {
	path := "/subjects/" + escapeSubjectPath(subject)
	if permanent {
		path += "?permanent=true"
	}
	var out []int
	if err := r.del(ctx, path, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Registry) post(ctx context.Context, path string, body any, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.base+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	return r.do(req, out)
}

func (r *Registry) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.base+path, nil)
	if err != nil {
		return err
	}
	return r.do(req, out)
}

func (r *Registry) put(ctx context.Context, path string, body any, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, r.base+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	return r.do(req, out)
}

func (r *Registry) del(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, r.base+path, nil)
	if err != nil {
		return err
	}
	return r.do(req, out)
}

func (r *Registry) do(req *http.Request, out any) error {
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	if r.user != "" {
		req.SetBasicAuth(r.user, r.pass)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, int64(limits.MaxHTTPBodyBytes())+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	if len(data) > limits.MaxHTTPBodyBytes() {
		return fmt.Errorf("schema: response body exceeds limit %d", limits.MaxHTTPBodyBytes())
	}
	if resp.StatusCode >= 300 {
		snippet := string(data)
		if len(snippet) > 512 {
			snippet = snippet[:512] + "..."
		}
		return fmt.Errorf("schema: HTTP %d: %s", resp.StatusCode, snippet)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

// doLookup runs a request that may legitimately 404 (schema/subject not found),
// returning ok=false with a nil error in that case rather than an error.
func (r *Registry) doLookup(req *http.Request, out any) (bool, error) {
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json")
	if r.user != "" {
		req.SetBasicAuth(r.user, r.pass)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, int64(limits.MaxHTTPBodyBytes())+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return false, err
	}
	if len(data) > limits.MaxHTTPBodyBytes() {
		return false, fmt.Errorf("schema: response body exceeds limit %d", limits.MaxHTTPBodyBytes())
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode >= 300 {
		snippet := string(data)
		if len(snippet) > 512 {
			snippet = snippet[:512] + "..."
		}
		return false, fmt.Errorf("schema: HTTP %d: %s", resp.StatusCode, snippet)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return false, err
		}
	}
	return true, nil
}

func escapeSubjectPath(subject string) string {
	return url.PathEscape(subject)
}
