package consumer

import (
	"sort"
	"sync"
)

// MemberInfo holds consumer registration details including unique member ID and subscribed topics.
type MemberInfo struct {
	MemberID string
	Topics   []string
}

// GroupState represents a consumer group's generation, active members, and rebalanced partition assignments.
type GroupState struct {
	GroupID     string
	Generation  int
	Members     map[string]*MemberInfo
	Assignments map[string]map[string][]int32
}

// GroupCoordinator manages multiple consumer group states and partition assignments.
type GroupCoordinator struct {
	mu     sync.RWMutex
	groups map[string]*GroupState
}

// NewGroupCoordinator initializes and returns a GroupCoordinator.
func NewGroupCoordinator() *GroupCoordinator {
	return &GroupCoordinator{
		groups: make(map[string]*GroupState),
	}
}

func minVal(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func equalSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// JoinGroup adds or updates a consumer member inside a group, returning the active generation ID.
func (gc *GroupCoordinator) JoinGroup(groupID, memberID string, topics []string) int {
	gc.mu.Lock()
	defer gc.mu.Unlock()

	gs, ok := gc.groups[groupID]
	if !ok {
		gs = &GroupState{
			GroupID:     groupID,
			Members:     make(map[string]*MemberInfo),
			Assignments: make(map[string]map[string][]int32),
		}
		gc.groups[groupID] = gs
	}

	memberChanged := false
	existing, ok := gs.Members[memberID]
	if !ok || !equalSlice(existing.Topics, topics) {
		memberChanged = true
	}

	gs.Members[memberID] = &MemberInfo{MemberID: memberID, Topics: topics}

	if memberChanged {
		gs.Generation++
		gc.rebalance(gs)
	}
	return gs.Generation
}

func (gc *GroupCoordinator) rebalance(gs *GroupState) {
	gs.Assignments = make(map[string]map[string][]int32)
	for mID := range gs.Members {
		gs.Assignments[mID] = make(map[string][]int32)
	}

	topicMembers := make(map[string][]string)
	for mID, mem := range gs.Members {
		for _, t := range mem.Topics {
			topicMembers[t] = append(topicMembers[t], mID)
		}
	}

	for topic, members := range topicMembers {
		sort.Strings(members)
		gc.assignTopicPartitions(gs, topic, members)
	}
}

func (gc *GroupCoordinator) assignTopicPartitions(gs *GroupState, topic string, members []string) {
	n := len(members)
	if n == 0 {
		return
	}
	p := 2
	limit := p / n
	rem := p % n

	for i := 0; i < n; i++ {
		numParts := limit
		if i < rem {
			numParts++
		}
		start := i*limit + minVal(i, rem)
		mID := members[i]
		for j := 0; j < numParts; j++ {
			part := int32(start + j)
			gs.Assignments[mID][topic] = append(gs.Assignments[mID][topic], part)
		}
	}
}

// SyncGroup returns the range-allocated partition assignments for a consumer group member.
func (gc *GroupCoordinator) SyncGroup(groupID, memberID string) map[string][]int32 {
	gc.mu.RLock()
	defer gc.mu.RUnlock()

	gs, ok := gc.groups[groupID]
	if !ok {
		return make(map[string][]int32)
	}
	if assignments, ok := gs.Assignments[memberID]; ok {
		return assignments
	}
	return make(map[string][]int32)
}
