package consumer

import (
	"testing"
)

func TestGroupCoordinatorRebalance(t *testing.T) {
	gc := NewGroupCoordinator()

	groupID := "test-group"
	topics := []string{"test-topic"}

	gen1 := gc.JoinGroup(groupID, "consumer-1", topics)
	if gen1 != 1 {
		t.Errorf("expected generation 1, got %d", gen1)
	}

	ass1 := gc.SyncGroup(groupID, "consumer-1")
	parts1 := ass1["test-topic"]
	if len(parts1) != 2 || parts1[0] != 0 || parts1[1] != 1 {
		t.Errorf("expected consumer-1 to get partitions 0 and 1, got %v", parts1)
	}

	gen2 := gc.JoinGroup(groupID, "consumer-2", topics)
	if gen2 != 2 {
		t.Errorf("expected generation 2, got %d", gen2)
	}

	ass1 = gc.SyncGroup(groupID, "consumer-1")
	parts1 = ass1["test-topic"]
	if len(parts1) != 1 || parts1[0] != 0 {
		t.Errorf("expected consumer-1 to get partition 0, got %v", parts1)
	}

	ass2 = gc.SyncGroup(groupID, "consumer-2")
	parts2 := ass2["test-topic"]
	if len(parts2) != 1 || parts2[0] != 1 {
		t.Errorf("expected consumer-2 to get partition 1, got %v", parts2)
	}
}
