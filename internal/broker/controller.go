package broker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Controller coordinates partition leadership assignments and automated health failovers.
type Controller struct {
	mu          sync.RWMutex
	brokerID    int
	dataDir     string
	metadataDir string
	leadersPath string
	lockPath    string
	leaders     map[string]int
	shutdown    chan struct{}
	wg          sync.WaitGroup
}

// NewController creates a new Controller instance for coordinating failover states.
func NewController(brokerID int, dataDir string) *Controller {
	metaDir := "/data/metadata"
	if _, err := os.Stat(metaDir); err != nil {
		metaDir = filepath.Join(dataDir, "metadata")
	}
	os.MkdirAll(metaDir, 0755)

	return &Controller{
		brokerID:    brokerID,
		dataDir:     dataDir,
		metadataDir: metaDir,
		leadersPath: filepath.Join(metaDir, "leaders.json"),
		lockPath:    filepath.Join(metaDir, "leaders.json.lock"),
		leaders:     make(map[string]int),
		shutdown:    make(chan struct{}),
	}
}

// Start launches the background leadership health monitoring loop.
func (c *Controller) Start() {
	c.mu.Lock()
	c.loadLeaders()
	c.mu.Unlock()
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runLoop()
	}()
}

// Close halts the controller loops.
func (c *Controller) Close() {
	c.Stop()
}

// Stop terminates background goroutines and blocks until all pending tasks complete.
func (c *Controller) Stop() {
	select {
	case <-c.shutdown:
		return
	default:
		close(c.shutdown)
	}
	c.wg.Wait()
}

func (c *Controller) runLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.checkFailover()
		case <-c.shutdown:
			return
		}
	}
}

func (c *Controller) loadLeaders() {
	file, err := os.Open(c.leadersPath)
	if err != nil {
		c.leaders["test-0"] = 0
		c.leaders["test-1"] = 1
		c.saveLeaders()
		return
	}
	defer file.Close()
	json.NewDecoder(file).Decode(&c.leaders)
}

func (c *Controller) saveLeaders() {
	file, err := os.OpenFile(c.leadersPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		return
	}
	defer file.Close()
	json.NewEncoder(file).Encode(c.leaders)
	file.Sync()
}

func isAlive(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (c *Controller) checkFailover() {
	c.mu.RLock()
	leadersCopy := make(map[string]int)
	for k, v := range c.leaders {
		leadersCopy[k] = v
	}
	c.mu.RUnlock()

	for partitionKey, leaderID := range leadersCopy {
		if leaderID == c.brokerID {
			continue
		}
		addr := fmt.Sprintf("broker-%d:9092", leaderID)
		if isAlive(addr) {
			continue
		}
		c.executeFailover(partitionKey)
	}
}

func (c *Controller) executeFailover(partitionKey string) {
	for {
		select {
		case <-c.shutdown:
			return
		default:
		}
		lockFile, err := os.OpenFile(c.lockPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
		if err == nil {
			lockFile.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer os.Remove(c.lockPath)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.loadLeaders()
	curr := c.leaders[partitionKey]
	if !isAlive(fmt.Sprintf("broker-%d:9092", curr)) {
		c.leaders[partitionKey] = c.brokerID
		c.saveLeaders()
		slog.Warn("Failover triggered", "partition", partitionKey, "new_leader", c.brokerID)
	}
}

// GetLeader retrieves the broker ID currently leading a specific partition.
func (c *Controller) GetLeader(topic string, partition int32) int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	partitionKey := fmt.Sprintf("%s-%d", topic, partition)
	if leaderID, ok := c.leaders[partitionKey]; ok {
		return leaderID
	}
	return int(partition % 2)
}

// GetLedPartitions returns a list of partition names led by the specified broker ID.
func (c *Controller) GetLedPartitions(brokerID int) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var list []string
	for k, v := range c.leaders {
		if v == brokerID {
			list = append(list, k)
		}
	}
	if list == nil {
		return []string{}
	}
	return list
}
