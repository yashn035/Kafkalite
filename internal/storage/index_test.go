package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSparseIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafkalite-index-test-*")
	if err != nil {
		t.Fatalf("failed temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	idxPath := filepath.Join(tmpDir, "test.index")
	idx, err := NewSparseIndex(idxPath)
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	if pos := idx.Lookup(10); pos != 0 {
		t.Errorf("expected position 0, got %d", pos)
	}

	if err := idx.AddEntry(0, 0); err != nil {
		t.Fatalf("failed add: %v", err)
	}
	if err := idx.AddEntry(10, 500); err != nil {
		t.Fatalf("failed add: %v", err)
	}
	if err := idx.AddEntry(20, 1200); err != nil {
		t.Fatalf("failed add: %v", err)
	}

	if pos := idx.Lookup(5); pos != 0 {
		t.Errorf("expected 0 for logical offset 5, got %d", pos)
	}
	if pos := idx.Lookup(10); pos != 500 {
		t.Errorf("expected 500 for logical offset 10, got %d", pos)
	}
	if pos := idx.Lookup(15); pos != 500 {
		t.Errorf("expected 500 for logical offset 15, got %d", pos)
	}
	if pos := idx.Lookup(20); pos != 1200 {
		t.Errorf("expected 1200 for logical offset 20, got %d", pos)
	}
	if pos := idx.Lookup(100); pos != 1200 {
		t.Errorf("expected 1200 for logical offset 100, got %d", pos)
	}

	idx.Close()

	idx2, err := NewSparseIndex(idxPath)
	if err != nil {
		t.Fatalf("failed to reload index: %v", err)
	}
	defer idx2.Close()

	if pos := idx2.Lookup(15); pos != 500 {
		t.Errorf("after reload, expected 500, got %d", pos)
	}
}
