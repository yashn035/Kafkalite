package broker

import (
	"bytes"
	"os"
	"testing"
)

func TestLogRetentionAndCompaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafkalite-retention-test-*")
	if err != nil {
		t.Fatalf("failed temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	s, err := NewServer(0, tmpDir)
	if err != nil {
		t.Fatalf("failed create server: %v", err)
	}
	defer s.Close()
	topic := "retention-topic"
	seg, err := s.getOrCreateTopicSegment(topic)
	if err != nil {
		t.Fatalf("failed get segment: %v", err)
	}

	seg.Append([]byte("k1"), []byte("v1"))
	seg.Append([]byte("k2"), []byte("v2"))
	seg.Append([]byte("k1"), []byte("v3"))

	records, _, err := seg.ReadRecords(0, 0)
	if err != nil || len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	if err := s.CompactTopic(topic); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	seg2, err := s.getOrCreateTopicSegment(topic)
	if err != nil {
		t.Fatalf("failed get segment: %v", err)
	}

	records2, _, err := seg2.ReadRecords(0, 0)
	if err != nil {
		t.Fatalf("failed read: %v", err)
	}

	if len(records2) != 2 {
		t.Fatalf("expected 2 records after compaction, got %d", len(records2))
	}

	if !bytes.Equal(records2[0].Key, []byte("k2")) || !bytes.Equal(records2[1].Key, []byte("k1")) || !bytes.Equal(records2[1].Value, []byte("v3")) {
		t.Errorf("records mismatch after compaction")
	}

	rm := NewRetentionManager(s, 0, 10)
	rm.processRetention()

	s.mu.Lock()
	_, ok := s.topics[topic]
	s.mu.Unlock()
	if ok {
		t.Errorf("expected topic log to be deleted by retention size limit")
	}
}
