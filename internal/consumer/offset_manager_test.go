package consumer

import (
	"os"
	"testing"
)

func TestOffsetStoreCommitAndFetch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafkalite-offsets-test-*")
	if err != nil {
		t.Fatalf("failed temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := NewOffsetStore(tmpDir)
	if err != nil {
		t.Fatalf("failed create store: %v", err)
	}

	groupID := "test-group"
	topic := "orders"
	var partition int32 = 0

	offset, err := store.FetchOffset(groupID, topic, partition)
	if err != nil {
		t.Fatalf("failed fetch offset: %v", err)
	}
	if offset != -1 {
		t.Errorf("expected offset -1, got %d", offset)
	}

	if err := store.CommitOffset(groupID, topic, partition, 125); err != nil {
		t.Fatalf("failed commit offset: %v", err)
	}

	offset, err = store.FetchOffset(groupID, topic, partition)
	if err != nil {
		t.Fatalf("failed fetch offset: %v", err)
	}
	if offset != 125 {
		t.Errorf("expected offset 125, got %d", offset)
	}
}
