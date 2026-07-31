package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSegment(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafkalite-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	segPath := filepath.Join(tmpDir, "test.log")
	seg, err := NewSegment(segPath, 100*time.Millisecond, 65536)
	if err != nil {
		t.Fatalf("failed to create segment: %v", err)
	}
	defer seg.Close()

	k1 := []byte("key1")
	v1 := []byte("value1")
	off1, err := seg.AppendBatch(k1, v1)
	if err != nil {
		t.Fatalf("failed append 1: %v", err)
	}
	if off1 != 0 {
		t.Errorf("expected offset 0, got %d", off1)
	}

	k2 := []byte("key-two")
	v2 := []byte("val-two-longer")
	off2, err := seg.AppendBatch(k2, v2)
	if err != nil {
		t.Fatalf("failed append 2: %v", err)
	}
	if off2 != 1 {
		t.Errorf("expected offset 1, got %d", off2)
	}

	rk1, rv1, nextOff, err := seg.ReadAt(off1)
	if err != nil {
		t.Fatalf("failed read 1: %v", err)
	}
	if !bytes.Equal(rk1, k1) || !bytes.Equal(rv1, v1) {
		t.Errorf("read 1 mismatch")
	}
	if nextOff != 1 {
		t.Errorf("expected next offset 1, got %d", nextOff)
	}

	rk2, rv2, _, err := seg.ReadAt(off2)
	if err != nil {
		t.Fatalf("failed read 2: %v", err)
	}
	if !bytes.Equal(rk2, k2) || !bytes.Equal(rv2, v2) {
		t.Errorf("read 2 mismatch")
	}

	records, nextBatchOff, err := seg.ReadRecords(0, 0)
	if err != nil {
		t.Fatalf("failed read records: %v", err)
	}
	if len(records) != 2 {
		t.Errorf("expected 2 records, got %d", len(records))
	}
	if records[0].Offset != 0 || !bytes.Equal(records[0].Key, k1) {
		t.Errorf("record 0 mismatch")
	}
	if records[1].Offset != 1 || !bytes.Equal(records[1].Key, k2) {
		t.Errorf("record 1 mismatch")
	}
	if nextBatchOff != 2 {
		t.Errorf("expected next batch offset 2, got %d", nextBatchOff)
	}
}
