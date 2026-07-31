package schema

import (
	"encoding/json"
	"fmt"
	"sync"
)

type Registry struct {
	mu      sync.RWMutex
	schemas map[string]map[string]interface{} // Topic -> JSON Schema
}

func NewRegistry() *Registry {
	return &Registry{
		schemas: make(map[string]map[string]interface{}),
	}
}

// Register parses and stores a JSON schema for a topic.
func (r *Registry) Register(topic string, schemaDef string) error {
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(schemaDef), &parsed); err != nil {
		return fmt.Errorf("invalid json schema: %w", err)
	}

	r.mu.Lock()
	r.schemas[topic] = parsed
	r.mu.Unlock()
	return nil
}

// Validate checks if a JSON payload matches the required top-level keys in the schema.
func (r *Registry) Validate(topic string, payload []byte) error {
	r.mu.RLock()
	schema, ok := r.schemas[topic]
	r.mu.RUnlock()

	if !ok {
		return nil // no schema enforced
	}

	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return fmt.Errorf("payload must be valid JSON: %w", err)
	}

	if req, ok := schema["required"].([]interface{}); ok {
		for _, v := range req {
			field := v.(string)
			if _, exists := data[field]; !exists {
				return fmt.Errorf("missing required field: %s", field)
			}
		}
	}
	return nil
}
