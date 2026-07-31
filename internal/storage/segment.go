// Package storage implements the append-only binary log segment storage and sparse indexes.
package storage

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
	"time"
)

// Record represents a single key-value message persisted in a segment file with a logical offset.
type Record struct {
	Timestamp int64
	Offset    int64
	Key       []byte
	Value     []byte
}

// Segment manages a single partition's append-only log file and its corresponding sparse offset index.
type Segment struct {
	mu              sync.RWMutex
	file            *os.File
	path            string
	size            int64
	index           *SparseIndex
	nextOffset      int64
	lastIndexedSize int64

	batchBuffer      *bytes.Buffer
	batchMutex       sync.Mutex
	flushTicker      *time.Ticker
	batchSize        int
	flushDone        chan struct{}
	batchStartOffset int64
}

func getIndexPath(logPath string) string {
	if len(logPath) > 4 && logPath[len(logPath)-4:] == ".log" {
		return logPath[:len(logPath)-4] + ".index"
	}
	return logPath + ".index"
}

// NewSegment instantiates a new Segment managing the raw log file and its index at the specified path.
func NewSegment(path string, flushInt time.Duration, batchSize int) (*Segment, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	idx, err := NewSparseIndex(getIndexPath(path))
	if err != nil {
		file.Close()
		return nil, err
	}
	s := &Segment{
		file:            file,
		path:            path,
		size:            info.Size(),
		index:           idx,
		batchBuffer:     new(bytes.Buffer),
		batchSize:       batchSize,
		flushTicker:     time.NewTicker(flushInt),
		flushDone:       make(chan struct{}),
	}
	if err := s.recoverNextOffset(); err != nil {
		s.Close()
		return nil, err
	}
	go s.flushLoop()
	return s, nil
}

func (s *Segment) recoverNextOffset() error {
	s.index.mu.RLock()
	n := len(s.index.entries)
	s.index.mu.RUnlock()

	var currPos int64 = 0
	var logicalOffset int64 = 0

	if n > 0 {
		s.index.mu.RLock()
		lastEntry := s.index.entries[n-1]
		s.index.mu.RUnlock()
		currPos = lastEntry.PhysicalPos
		logicalOffset = lastEntry.LogicalOffset
	}

	for currPos < s.size {
		_, _, _, next, err := s.readAtNoLock(currPos)
		if err != nil {
			break
		}
		logicalOffset++
		currPos = next
	}
	s.nextOffset = logicalOffset
	s.lastIndexedSize = currPos
	return nil
}

// Close safely closes the sparse index and commits any remaining writes to the log file descriptor.
func (s *Segment) Close() error {
	s.flushTicker.Stop()
	close(s.flushDone)
	s.flush()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.index.Close()
	return s.file.Close()
}

// Size returns the active physical size of the raw segment file.
func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

func (s *Segment) flushLoop() {
	for {
		select {
		case <-s.flushTicker.C:
			s.flush()
		case <-s.flushDone:
			return
		}
	}
}

func (s *Segment) flush() error {
	s.batchMutex.Lock()
	defer s.batchMutex.Unlock()

	if s.batchBuffer.Len() == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	buf := s.batchBuffer.Bytes()
	n, err := s.file.Write(buf)
	if err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}

	physicalPos := s.size
	s.size += int64(n)

	// Since we batched multiple records, add one index entry for the first record in the batch.
	if physicalPos == 0 || s.size-s.lastIndexedSize >= 4096 {
		if err := s.index.AddEntry(s.batchStartOffset, physicalPos); err != nil {
			return err
		}
		s.lastIndexedSize = s.size
	}
	
	s.batchBuffer.Reset()
	return nil
}

// AppendBatch writes a new key-value pair to the log segment buffer and triggers a flush if needed.
func (s *Segment) AppendBatch(key, value []byte) (int64, error) {
	s.batchMutex.Lock()

	s.mu.Lock()
	if s.batchBuffer.Len() == 0 {
		s.batchStartOffset = s.nextOffset
	}
	logicalOffset := s.nextOffset
	s.nextOffset++
	s.mu.Unlock()

	kLen := int32(len(key))
	vLen := int32(len(value))
	timestamp := time.Now().UnixMilli()

	buf := make([]byte, 8+4+len(key)+4+len(value))
	binary.BigEndian.PutUint64(buf[0:8], uint64(timestamp))
	binary.BigEndian.PutUint32(buf[8:12], uint32(kLen))
	copy(buf[12:12+len(key)], key)
	binary.BigEndian.PutUint32(buf[12+len(key):16+len(key)], uint32(vLen))
	copy(buf[16+len(key):], value)

	s.batchBuffer.Write(buf)
	shouldFlush := s.batchBuffer.Len() >= s.batchSize
	s.batchMutex.Unlock()

	if shouldFlush {
		s.flush()
	}
	return logicalOffset, nil
}

func (s *Segment) readAtNoLock(offset int64) (int64, []byte, []byte, int64, error) {
	tsBuf := make([]byte, 8)
	if _, err := s.file.ReadAt(tsBuf, offset); err != nil {
		return 0, nil, nil, 0, err
	}
	timestamp := int64(binary.BigEndian.Uint64(tsBuf))

	keyLenBuf := make([]byte, 4)
	if _, err := s.file.ReadAt(keyLenBuf, offset+8); err != nil {
		return 0, nil, nil, 0, err
	}
	kLen := binary.BigEndian.Uint32(keyLenBuf)
	key := make([]byte, kLen)
	if _, err := s.file.ReadAt(key, offset+12); err != nil {
		return 0, nil, nil, 0, err
	}
	valLenBuf := make([]byte, 4)
	if _, err := s.file.ReadAt(valLenBuf, offset+12+int64(kLen)); err != nil {
		return 0, nil, nil, 0, err
	}
	vLen := binary.BigEndian.Uint32(valLenBuf)
	value := make([]byte, vLen)
	if _, err := s.file.ReadAt(value, offset+12+int64(kLen)+4); err != nil {
		return 0, nil, nil, 0, err
	}
	return timestamp, key, value, offset + 12 + int64(kLen) + 4 + int64(vLen), nil
}

func (s *Segment) seekPhysicalPos(targetLogical int64) (int64, int64, error) {
	entry := s.index.LookupEntry(targetLogical)
	currPos := entry.PhysicalPos
	currLogical := entry.LogicalOffset
	for currLogical < targetLogical {
		if currPos >= s.size {
			return currPos, currLogical, nil
		}
		_, _, _, next, err := s.readAtNoLock(currPos)
		if err != nil {
			return currPos, currLogical, err
		}
		currLogical++
		currPos = next
	}
	return currPos, currLogical, nil
}

// ReadAt fetches a single record at a specific logical offset.
func (s *Segment) ReadAt(logicalOffset int64) ([]byte, []byte, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	currPos, _, err := s.seekPhysicalPos(logicalOffset)
	if err != nil {
		return nil, nil, 0, err
	}
	if currPos >= s.size {
		return nil, nil, 0, io.EOF
	}
	ts, key, val, _, err := s.readAtNoLock(currPos)
	_ = ts
	if err != nil {
		return nil, nil, 0, err
	}
	return key, val, logicalOffset + 1, nil
}

// ReadRecords retrieves a batch of records starting from a logical offset up to maxBytes.
func (s *Segment) ReadRecords(startLogical int64, maxBytes int) ([]Record, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	currPos, currLogical, err := s.seekPhysicalPos(startLogical)
	if err != nil {
		return nil, startLogical, err
	}

	var records []Record
	bytesRead := 0
	for {
		if currPos >= s.size || (maxBytes > 0 && bytesRead >= maxBytes) {
			break
		}
		ts, key, val, next, err := s.readAtNoLock(currPos)
		if err != nil {
			break
		}
		records = append(records, Record{Timestamp: ts, Offset: currLogical, Key: key, Value: val})
		bytesRead += int(next - currPos)
		currLogical++
		currPos = next
	}
	return records, currLogical, nil
}
