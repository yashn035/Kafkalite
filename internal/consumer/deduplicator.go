package consumer

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Deduplicator tracks processed message IDs per consumer group.
type Deduplicator struct {
	mu          sync.RWMutex
	processed   map[string]map[string]bool // GroupID -> MessageID -> processed
	dataDir     string
}

func NewDeduplicator(dataDir string) *Deduplicator {
	d := &Deduplicator{
		processed: make(map[string]map[string]bool),
		dataDir:   dataDir,
	}
	_ = os.MkdirAll(dataDir, 0755)
	return d
}

// Load loads processed IDs from disk for a group.
func (d *Deduplicator) Load(groupID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.processed[groupID]; !ok {
		d.processed[groupID] = make(map[string]bool)
	}
	path := filepath.Join(d.dataDir, "processed_"+groupID+".json")
	data, err := os.ReadFile(path)
	if err == nil {
		var ids []string
		if json.Unmarshal(data, &ids) == nil {
			for _, id := range ids {
				d.processed[groupID][id] = true
			}
		}
	}
}

// Save saves processed IDs to disk for a group.
func (d *Deduplicator) Save(groupID string) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	ids := make([]string, 0, len(d.processed[groupID]))
	for id := range d.processed[groupID] {
		ids = append(ids, id)
	}
	data, err := json.Marshal(ids)
	if err == nil {
		path := filepath.Join(d.dataDir, "processed_"+groupID+".json")
		_ = os.WriteFile(path, data, 0644)
	}
}

// MarkProcessed marks a list of IDs as processed.
func (d *Deduplicator) MarkProcessed(groupID string, ids []string) {
	if len(ids) == 0 {
		return
	}
	d.mu.Lock()
	if _, ok := d.processed[groupID]; !ok {
		d.processed[groupID] = make(map[string]bool)
	}
	for _, id := range ids {
		d.processed[groupID][id] = true
	}
	d.mu.Unlock()
	d.Save(groupID)
}

// FilterProcessed returns only those IDs that have NOT been processed yet.
// Wait, the broker should filter them. We'll provide IsProcessed.
func (d *Deduplicator) IsProcessed(groupID string, messageID string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if grp, ok := d.processed[groupID]; ok {
		return grp[messageID]
	}
	return false
}
