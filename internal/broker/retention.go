package broker

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kafkalite/internal/storage"
)

type RetentionManager struct {
	server             *Server
	retentionMs        int64
	retentionBytes     int64
	compactionInterval time.Duration
}

func NewRetentionManager(s *Server, ms, bytes int64, compInt time.Duration) *RetentionManager {
	return &RetentionManager{
		server:             s,
		retentionMs:        ms,
		retentionBytes:     bytes,
		compactionInterval: compInt,
	}
}

func (rm *RetentionManager) Start(shutdown chan struct{}) {
	retentionTicker := time.NewTicker(1 * time.Minute)
	go func() {
		for {
			select {
			case <-retentionTicker.C:
				rm.processRetention()
			case <-shutdown:
				retentionTicker.Stop()
				return
			}
		}
	}()

	compactionTicker := time.NewTicker(rm.compactionInterval)
	go func() {
		for {
			select {
			case <-compactionTicker.C:
				rm.processCompaction(1000)
			case <-shutdown:
				compactionTicker.Stop()
				return
			}
		}
	}()
}

func (rm *RetentionManager) processRetention() {
	files, err := os.ReadDir(rm.server.dataDir)
	if err != nil {
		return
	}
	now := time.Now()
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".log") {
			continue
		}
		info, err := f.Info()
		if err != nil {
			continue
		}
		topic := strings.TrimSuffix(f.Name(), ".log")
		if rm.retentionBytes > 0 && info.Size() > rm.retentionBytes {
			rm.deleteTopicLog(topic)
			continue
		}
		if rm.retentionMs > 0 && now.Sub(info.ModTime()).Milliseconds() > rm.retentionMs {
			rm.deleteTopicLog(topic)
		}
	}
}

func (rm *RetentionManager) deleteTopicLog(topic string) {
	rm.server.mu.Lock()
	seg, ok := rm.server.topics[topic]
	if ok {
		seg.Close()
		delete(rm.server.topics, topic)
	}
	rm.server.mu.Unlock()

	logPath := filepath.Join(rm.server.dataDir, topic+".log")
	os.Remove(logPath)
	os.Remove(logPath + ".index")
	log.Printf("Retention deleted log for topic: %s", topic)
}

func (rm *RetentionManager) processCompaction(threshold int) {
	rm.server.mu.Lock()
	var topicsToCompact []string
	for topic, seg := range rm.server.topics {
		records, _, err := seg.ReadRecords(0, 0)
		if err == nil && len(records) >= threshold {
			topicsToCompact = append(topicsToCompact, topic)
		}
	}
	rm.server.mu.Unlock()

	for _, topic := range topicsToCompact {
		if err := rm.server.CompactTopic(topic); err != nil {
			log.Printf("Compaction failed for topic %s: %v", topic, err)
		} else {
			log.Printf("Successfully compacted topic: %s", topic)
		}
	}
}

func (s *Server) CompactTopic(topic string) error {
	s.mu.Lock()
	seg, ok := s.topics[topic]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	records, _, err := seg.ReadRecords(0, 0)
	if err != nil || len(records) < 2 {
		return err
	}
	lastOffsets := getCompactionOffsets(records)
	if err := s.writeCompactedFile(topic, records, lastOffsets); err != nil {
		return err
	}
	seg.Close()
	return s.replaceWithCompacted(topic)
}

func getCompactionOffsets(records []storage.Record) map[string]int64 {
	lastOffsets := make(map[string]int64)
	for _, rec := range records {
		if len(rec.Key) > 0 {
			lastOffsets[string(rec.Key)] = rec.Offset
		}
	}
	return lastOffsets
}

func (s *Server) writeCompactedFile(topic string, records []storage.Record, lastOffsets map[string]int64) error {
	tempPath := filepath.Join(s.dataDir, fmt.Sprintf("%s.compacted.log", topic))
	os.Remove(tempPath)
	os.Remove(tempPath + ".index")
	tempSeg, err := storage.NewSegment(tempPath, s.flushInt, s.batchSize)
	if err != nil {
		return err
	}
	defer tempSeg.Close()
	for _, rec := range records {
		if len(rec.Key) == 0 || rec.Offset == lastOffsets[string(rec.Key)] {
			if _, err := tempSeg.AppendBatch(rec.Key, rec.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Server) replaceWithCompacted(topic string) error {
	logPath := filepath.Join(s.dataDir, fmt.Sprintf("%s.log", topic))
	idxPath := logPath + ".index"
	tempPath := filepath.Join(s.dataDir, fmt.Sprintf("%s.compacted.log", topic))

	os.Remove(logPath)
	os.Remove(idxPath)
	os.Rename(tempPath, logPath)
	os.Rename(tempPath+".index", idxPath)

	newSeg, err := storage.NewSegment(logPath, s.flushInt, s.batchSize)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.topics[topic] = newSeg
	s.mu.Unlock()
	return nil
}
