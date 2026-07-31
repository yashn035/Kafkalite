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
	leaders     map[string]int
	activeNodes map[int]time.Time
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
		leaders:     make(map[string]int),
		activeNodes: make(map[int]time.Time),
		shutdown:    make(chan struct{}),
	}
}

// Start launches the background leadership health monitoring loop.
func (c *Controller) Start() {
	c.mu.Lock()
	c.loadLeaders()
	c.mu.Unlock()
	c.wg.Add(2)
	go func() {
		defer c.wg.Done()
		c.heartbeatServer()
	}()
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
	ticker := time.NewTicker(2 * time.Second)
	failoverTicker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	defer failoverTicker.Stop()

	for {
		select {
		case <-ticker.C:
			c.sendHeartbeats()
		case <-failoverTicker.C:
			c.checkFailover()
		case <-c.shutdown:
			return
		}
	}
}

func (c *Controller) sendHeartbeats() {
	msg := fmt.Appendf(nil, "%d", c.brokerID)
	// Hardcoded 3 brokers for demo
	for i := 0; i < 3; i++ {
		addr := fmt.Sprintf("localhost:%d", 9095+i)
		conn, err := net.Dial("udp", addr)
		if err == nil {
			conn.Write(msg)
			conn.Close()
		}
	}
}

func (c *Controller) heartbeatServer() {
	addr := fmt.Sprintf("0.0.0.0:%d", 9095+c.brokerID)
	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		slog.Error("Failed to start heartbeat server", "err", err)
		return
	}
	defer conn.Close()

	buf := make([]byte, 1024)
	for {
		select {
		case <-c.shutdown:
			return
		default:
		}
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, _, err := conn.ReadFrom(buf)
		if err == nil {
			var id int
			fmt.Sscanf(string(buf[:n]), "%d", &id)
			c.mu.Lock()
			c.activeNodes[id] = time.Now()
			c.mu.Unlock()
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

func (c *Controller) checkFailover() {
	c.mu.RLock()
	leadersCopy := make(map[string]int)
	for k, v := range c.leaders {
		leadersCopy[k] = v
	}
	activeCopy := make(map[int]time.Time)
	for k, v := range c.activeNodes {
		activeCopy[k] = v
	}
	c.mu.RUnlock()

	// Ensure self is active
	activeCopy[c.brokerID] = time.Now()

	for partitionKey, leaderID := range leadersCopy {
		if leaderID == c.brokerID {
			continue
		}
		
		lastSeen, ok := activeCopy[leaderID]
		// If leader hasn't sent a heartbeat in 10 seconds, it's dead
		if !ok || time.Since(lastSeen) > 10*time.Second {
			c.executeFailover(partitionKey, activeCopy)
		}
	}
}

func (c *Controller) executeFailover(partitionKey string, active map[int]time.Time) {
	// Find lowest active ID
	lowestID := c.brokerID
	for id, lastSeen := range active {
		if time.Since(lastSeen) <= 10*time.Second && id < lowestID {
			lowestID = id
		}
	}

	// Only the node with the lowest ID gets to claim leadership to prevent split-brain
	if lowestID != c.brokerID {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.loadLeaders()
	
	// Double check if it changed
	curr := c.leaders[partitionKey]
	lastSeen, ok := active[curr]
	if !ok || time.Since(lastSeen) > 10*time.Second {
		c.leaders[partitionKey] = c.brokerID
		c.saveLeaders()
		slog.Warn("Lease Failover triggered", "partition", partitionKey, "new_leader", c.brokerID)
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

// MovePartition explicitly reassigns a partition's leadership to a new broker.
func (c *Controller) MovePartition(partitionKey string, newLeaderID int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loadLeaders()
	
	if current, exists := c.leaders[partitionKey]; exists && current == newLeaderID {
		return // already the leader
	}

	c.leaders[partitionKey] = newLeaderID
	c.saveLeaders()
	slog.Info("Partition moved via rebalancer", "partition", partitionKey, "new_leader", newLeaderID)
}
