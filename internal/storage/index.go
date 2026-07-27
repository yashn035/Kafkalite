package storage

import (
	"encoding/binary"
	"os"
	"sync"
)

// IndexEntry maps a logical offset to its corresponding physical byte position in the log file.
type IndexEntry struct {
	LogicalOffset int64
	PhysicalPos   int64
}

// SparseIndex maintains an index of checkpoints on disk to speed up message lookups.
type SparseIndex struct {
	mu       sync.RWMutex
	file     *os.File
	path     string
	entries  []IndexEntry
	lastSize int64
}

// NewSparseIndex creates or loads a sparse index file from the specified path.
func NewSparseIndex(path string) (*SparseIndex, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	idx := &SparseIndex{
		file: file,
		path: path,
	}
	if err := idx.loadEntries(); err != nil {
		file.Close()
		return nil, err
	}
	return idx, nil
}

// Close closes the underlying index file descriptor safely.
func (idx *SparseIndex) Close() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	return idx.file.Close()
}

// Clear truncates the index file and resets the memory slice cache.
func (idx *SparseIndex) Clear() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.entries = nil
	idx.lastSize = 0
	if err := idx.file.Truncate(0); err != nil {
		return err
	}
	return idx.file.Sync()
}

func (idx *SparseIndex) loadEntries() error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	info, err := idx.file.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	idx.lastSize = size

	numEntries := size / 16
	idx.entries = make([]IndexEntry, numEntries)
	buf := make([]byte, 16)
	for i := int64(0); i < numEntries; i++ {
		if _, err := idx.file.ReadAt(buf, i*16); err != nil {
			return err
		}
		idx.entries[i] = IndexEntry{
			LogicalOffset: int64(binary.BigEndian.Uint64(buf[0:8])),
			PhysicalPos:   int64(binary.BigEndian.Uint64(buf[8:16])),
		}
	}
	return nil
}

// AddEntry appends a new checkpoint record mapping logical offset to physical position.
func (idx *SparseIndex) AddEntry(logicalOffset, physicalPos int64) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[0:8], uint64(logicalOffset))
	binary.BigEndian.PutUint64(buf[8:16], uint64(physicalPos))

	if _, err := idx.file.WriteAt(buf, idx.lastSize); err != nil {
		return err
	}
	if err := idx.file.Sync(); err != nil {
		return err
	}
	idx.lastSize += 16
	idx.entries = append(idx.entries, IndexEntry{
		LogicalOffset: logicalOffset,
		PhysicalPos:   physicalPos,
	})
	return nil
}

// Lookup binary-searches the index entries and returns the nearest physical position.
func (idx *SparseIndex) Lookup(logicalOffset int64) int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return 0
	}

	low, high := 0, len(idx.entries)-1
	ans := -1
	for low <= high {
		mid := low + (high-low)/2
		if idx.entries[mid].LogicalOffset <= logicalOffset {
			ans = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if ans == -1 {
		return 0
	}
	return idx.entries[ans].PhysicalPos
}

// LookupEntry returns the complete nearest checkpoint IndexEntry for a logical offset.
func (idx *SparseIndex) LookupEntry(logicalOffset int64) IndexEntry {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.entries) == 0 {
		return IndexEntry{LogicalOffset: 0, PhysicalPos: 0}
	}

	low, high := 0, len(idx.entries)-1
	ans := -1
	for low <= high {
		mid := low + (high-low)/2
		if idx.entries[mid].LogicalOffset <= logicalOffset {
			ans = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if ans == -1 {
		return IndexEntry{LogicalOffset: 0, PhysicalPos: 0}
	}
	return idx.entries[ans]
}
