package broker

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"kafkalite/internal/protocol"
)

type Replica struct {
	ID   string
	Conn net.Conn
	mu   sync.Mutex
}

type ReplicationManager struct {
	mu       sync.Mutex
	replicas map[string]*Replica
}

func NewReplicationManager() *ReplicationManager {
	return &ReplicationManager{
		replicas: make(map[string]*Replica),
	}
}

func (rm *ReplicationManager) RegisterReplica(id string, conn net.Conn) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if old, ok := rm.replicas[id]; ok {
		old.Conn.Close()
	}
	rm.replicas[id] = &Replica{ID: id, Conn: conn}
}

func (rm *ReplicationManager) RemoveReplica(id string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rep, ok := rm.replicas[id]; ok {
		rep.Conn.Close()
		delete(rm.replicas, id)
	}
}

func (rm *ReplicationManager) replicateToReplica(ctx context.Context, rep *Replica, topic string, offset int64, key, value []byte) error {
	rep.mu.Lock()
	defer rep.mu.Unlock()

	deadline, _ := ctx.Deadline()
	rep.Conn.SetDeadline(deadline)

	req := &protocol.Request{
		Type:   protocol.ReqReplicate,
		Topic:  topic,
		Offset: offset,
		Key:    key,
		Value:  value,
	}

	if err := protocol.WriteRequest(rep.Conn, req); err != nil {
		return err
	}
	resp, err := protocol.ReadResponse(rep.Conn, false)
	if err != nil {
		return err
	}
	if resp.Status != protocol.StatusOk {
		return errors.New("replica returned error status")
	}
	return nil
}

func (rm *ReplicationManager) Replicate(topic string, offset int64, key, value []byte) error {
	rm.mu.Lock()
	activeReplicas := make([]*Replica, 0, len(rm.replicas))
	for _, rep := range rm.replicas {
		activeReplicas = append(activeReplicas, rep)
	}
	rm.mu.Unlock()

	if len(activeReplicas) == 0 {
		return nil
	}

	ackChan := make(chan string, len(activeReplicas))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	for _, r := range activeReplicas {
		go func(rep *Replica) {
			if err := rm.replicateToReplica(ctx, rep, topic, offset, key, value); err != nil {
				rm.RemoveReplica(rep.ID)
			} else {
				ackChan <- rep.ID
			}
		}(r)
	}

	select {
	case <-ackChan:
		return nil
	case <-ctx.Done():
		return errors.New("replication timeout waiting for 1 ACK")
	}
}

func (s *Server) RunFollowerLoop(leaderAddr string, nodeID string) {
	for {
		select {
		case <-s.shutdown:
			return
		default:
		}
		conn, err := net.Dial("tcp", leaderAddr)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		if err := s.handleFollowerHandshakeAndLoop(conn, nodeID); err != nil {
			conn.Close()
			time.Sleep(1 * time.Second)
		}
	}
}

func (s *Server) handleFollowerHandshakeAndLoop(conn net.Conn, nodeID string) error {
	req := &protocol.Request{
		Type:  protocol.ReqJoinReplica,
		Topic: nodeID,
	}
	if err := protocol.WriteRequest(conn, req); err != nil {
		return err
	}
	for {
		req, err := protocol.ReadRequest(conn)
		if err != nil {
			return err
		}
		if req.Type != protocol.ReqReplicate {
			return errors.New("follower expected ReqReplicate")
		}
		seg, err := s.getOrCreateTopicSegment(req.Topic)
		var resp protocol.Response
		if err != nil {
			resp = protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
		} else {
			_, err = seg.AppendBatch(req.Key, req.Value)
			if err != nil {
				resp = protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
			} else {
				resp = protocol.Response{Status: protocol.StatusOk}
			}
		}
		if err := protocol.WriteResponse(conn, &resp); err != nil {
			return err
		}
	}
}
