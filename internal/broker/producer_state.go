package broker

import (
	"sync"
)

// ProducerStateManager tracks the highest sequence number and offset for each producer to ensure exactly-once semantics.
type ProducerStateManager struct {
	mu     sync.RWMutex
	states map[int64]map[int32]int64 // ProducerID -> SequenceNumber -> Offset
}

func NewProducerStateManager() *ProducerStateManager {
	return &ProducerStateManager{
		states: make(map[int64]map[int32]int64),
	}
}

// CheckDuplicate returns the stored offset and true if the message was already processed.
func (p *ProducerStateManager) CheckDuplicate(producerID int64, seqNum int32) (int64, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if prodMap, ok := p.states[producerID]; ok {
		if offset, exists := prodMap[seqNum]; exists {
			return offset, true
		}
	}
	return 0, false
}

// RecordSequence stores the successful offset for a given producer sequence.
func (p *ProducerStateManager) RecordSequence(producerID int64, seqNum int32, offset int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, ok := p.states[producerID]; !ok {
		p.states[producerID] = make(map[int32]int64)
	}
	p.states[producerID][seqNum] = offset
}
