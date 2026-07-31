package producer

import (
	"sync"
	"time"
)

type entry struct {
	offset    int64
	timestamp time.Time
}

// IdempotentManager tracks processed message IDs per producer to ensure exactly-once semantics.
type IdempotentManager struct {
	mu     sync.RWMutex
	states map[int64]map[string]entry // ProducerID -> MessageID -> Offset + Timestamp
	ttl    time.Duration
}

func NewIdempotentManager(ttl time.Duration) *IdempotentManager {
	return &IdempotentManager{
		states: make(map[int64]map[string]entry),
		ttl:    ttl,
	}
}

// CheckDuplicate returns the stored offset and true if the message ID was already processed.
func (m *IdempotentManager) CheckDuplicate(producerID int64, messageID string) (int64, bool) {
	if messageID == "" {
		return 0, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	if prodMap, ok := m.states[producerID]; ok {
		if ent, exists := prodMap[messageID]; exists {
			if time.Since(ent.timestamp) <= m.ttl {
				return ent.offset, true
			}
		}
	}
	return 0, false
}

// RecordMessage stores the successful offset for a given producer message ID.
func (m *IdempotentManager) RecordMessage(producerID int64, messageID string, offset int64) {
	if messageID == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.states[producerID]; !ok {
		m.states[producerID] = make(map[string]entry)
	}
	m.states[producerID][messageID] = entry{
		offset:    offset,
		timestamp: time.Now(),
	}
}
