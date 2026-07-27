package consumer

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type OffsetRecord struct {
	Offset int64 `json:"offset"`
}

type OffsetStore struct {
	mu      sync.RWMutex
	baseDir string
}

func NewOffsetStore(baseDir string) (*OffsetStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	return &OffsetStore{
		baseDir: baseDir,
	}, nil
}

func (o *OffsetStore) getFilePath(groupID, topic string, partition int32) string {
	return filepath.Join(o.baseDir, "offsets", groupID, fmt.Sprintf("%s_%d.json", topic, partition))
}

func (o *OffsetStore) CommitOffset(groupID, topic string, partition int32, offset int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	path := o.getFilePath(groupID, topic, partition)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	rec := OffsetRecord{Offset: offset}
	enc := json.NewEncoder(file)
	if err := enc.Encode(rec); err != nil {
		return err
	}

	return file.Sync()
}

func (o *OffsetStore) FetchOffset(groupID, topic string, partition int32) (int64, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	path := o.getFilePath(groupID, topic, partition)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return -1, nil
		}
		return -1, err
	}
	defer file.Close()

	var rec OffsetRecord
	dec := json.NewDecoder(file)
	if err := dec.Decode(&rec); err != nil {
		return -1, err
	}
	return rec.Offset, nil
}
