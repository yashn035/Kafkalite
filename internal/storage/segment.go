// Package storage implements the append-only binary log segment storage and sparse indexes.
package storage

import (
	"encoding/binary"
	"io"
	"os"
	"sync"
)

// Record represents a single key-value message persisted in a segment file with a logical offset.
type Record struct {
	Offset int64
	Key    []byte
	Value  []byte
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
}

func getIndexPath(logPath string) string {
	if len(logPath) > 4 && logPath[len(logPath)-4:] == ".log" {
		return logPath[:len(logPath)-4] + ".index"
	}
	return logPath + ".index"
}

// NewSegment instantiates a new Segment managing the raw log file and its index at the specified path.
func NewSegment(path string) (*Segment, error) {
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
		file: file,
		path: path,
		size: info.Size(),
		index: idx,
	}
	if err := s.recoverNextOffset(); err != nil {
		s.Close()
		return nil, err
	}
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
		_, _, next, err := s.readAtNoLock(currPos)
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

// Append writes a new key-value pair to the log segment, flushes to disk (fsync), and commits index checkpoints.
func (s *Segment) Append(key, value []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logicalOffset := s.nextOffset
	physicalPos := s.size
	kLen := int32(len(key))
	vLen := int32(len(value))

	buf := make([]byte, 8+len(key)+len(value))
	binary.BigEndian.PutUint32(buf[0:4], uint32(kLen))
	copy(buf[4:4+len(key)], key)
	binary.BigEndian.PutUint32(buf[4+len(key):8+len(key)], uint32(vLen))
	copy(buf[8+len(key):], value)

	n, err := s.file.Write(buf)
	if err != nil {
		return 0, err
	}
	if err := s.file.Sync(); err != nil {
		return 0, err
	}
	s.size += int64(n)
	s.nextOffset++

	if physicalPos == 0 || s.size-s.lastIndexedSize >= 4096 {
		if err := s.index.AddEntry(logicalOffset, physicalPos); err != nil {
			return 0, err
		}
		s.lastIndexedSize = s.size
	}
	return logicalOffset, nil
}

func (s *Segment) readAtNoLock(offset int64) ([]byte, []byte, int64, error) {
	keyLenBuf := make([]byte, 4)
	if _, err := s.file.ReadAt(keyLenBuf, offset); err != nil {
		return nil, nil, 0, err
	}
	kLen := binary.BigEndian.Uint32(keyLenBuf)
	key := make([]byte, kLen)
	if _, err := s.file.ReadAt(key, offset+4); err != nil {
		return nil, nil, 0, err
	}
	valLenBuf := make([]byte, 4)
	if _, err := s.file.ReadAt(valLenBuf, offset+4+int64(kLen)); err != nil {
		return nil, nil, 0, err
	}
	vLen := binary.BigEndian.Uint32(valLenBuf)
	value := make([]byte, vLen)
	if _, err := s.file.ReadAt(value, offset+4+int64(kLen)+4); err != nil {
		return nil, nil, 0, err
	}
	return key, value, offset + 4 + int64(kLen) + 4 + int64(vLen), nil
}

func (s *Segment) seekPhysicalPos(targetLogical int64) (int64, int64, error) {
	entry := s.index.LookupEntry(targetLogical)
	currPos := entry.PhysicalPos
	currLogical := entry.LogicalOffset
	for currLogical < targetLogical {
		if currPos >= s.size {
			return currPos, currLogical, nil
		}
		_, _, next, err := s.readAtNoLock(currPos)
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
	key, val, _, err := s.readAtNoLock(currPos)
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
		key, val, next, err := s.readAtNoLock(currPos)
		if err != nil {
			break
		}
		records = append(records, Record{Offset: currLogical, Key: key, Value: val})
		bytesRead += int(next - currPos)
		currLogical++
		currPos = next
	}
	return records, currLogical, nil
}
