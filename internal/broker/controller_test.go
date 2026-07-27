package broker

import (
	"os"
	"testing"
)

func TestControllerLeadersSaveAndLoad(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafkalite-controller-test-*")
	if err != nil {
		t.Fatalf("failed temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	c := NewController(1, tmpDir)
	defer c.Close()

	c.Start()

	l0 := c.GetLeader("test", 0)
	l1 := c.GetLeader("test", 1)
	if l0 != 0 || l1 != 1 {
		t.Errorf("expected leaders 0 and 1, got %d and %d", l0, l1)
	}

	c.mu.Lock()
	c.leaders["test-0"] = 1
	c.saveLeaders()
	c.mu.Unlock()

	c2 := NewController(2, tmpDir)
	defer c2.Close()
	c2.loadLeaders()

	if leader := c2.GetLeader("test", 0); leader != 1 {
		t.Errorf("expected reloaded leader 1, got %d", leader)
	}
}
