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
	limited := io.LimitReader(resp.Body, int64(limits.MaxHTTPBodyBytes)+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	if len(data) > limits.MaxHTTPBodyBytes {
		return fmt.Errorf("schema: response body exceeds limit %d", limits.MaxHTTPBodyBytes)
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

func escapeSubjectPath(subject string) string {
	return url.PathEscape(subject)
}
