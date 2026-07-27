package broker

import (
	"bytes"
	"net"
	"os"
	"testing"
	"time"

	"kafkalite/internal/protocol"
)

func TestReplicationLeaderFollower(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafkalite-repl-test-*")
	if err != nil {
		t.Fatalf("failed temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	leader, err := NewServer(0, tmpDir+"/leader")
	if err != nil {
		t.Fatalf("leader create fail: %v", err)
	}
	defer leader.Close()

	follower, err := NewServer(1, tmpDir+"/follower")
	if err != nil {
		t.Fatalf("follower create fail: %v", err)
	}
	defer follower.Close()
	l1, _ := net.Listen("tcp", "127.0.0.1:0")
	leaderAddr := l1.Addr().String()
	l1.Close()

	l2, _ := net.Listen("tcp", "127.0.0.1:0")
	followerAddr := l2.Addr().String()
	l2.Close()

	go func() { leader.Start(leaderAddr) }()
	go func() { follower.Start(followerAddr) }()
	time.Sleep(100 * time.Millisecond)

	go follower.RunFollowerLoop(leaderAddr, "follower-node")
	time.Sleep(200 * time.Millisecond)

	clientConn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		t.Fatalf("connect leader fail: %v", err)
	}
	defer clientConn.Close()

	topic := "replicated-topic"
	key := []byte("repl-key")
	value := []byte("repl-val")

	req := &protocol.Request{
		Type:  protocol.ReqProduce,
		Topic: topic,
		Key:   key,
		Value: value,
	}
	if err := protocol.WriteRequest(clientConn, req); err != nil {
		t.Fatalf("produce write fail: %v", err)
	}

	resp, err := protocol.ReadResponse(clientConn, false)
	if err != nil {
		t.Fatalf("produce read resp fail: %v", err)
	}
	if resp.Status != protocol.StatusOk {
		t.Fatalf("produce error: %s", resp.ErrMsg)
	}

	followerConn, err := net.Dial("tcp", followerAddr)
	if err != nil {
		t.Fatalf("connect follower fail: %v", err)
	}
	defer followerConn.Close()

	consReq := &protocol.Request{
		Type:     protocol.ReqConsume,
		Topic:    topic,
		Offset:   0,
		MaxBytes: 1024,
	}
	if err := protocol.WriteRequest(followerConn, consReq); err != nil {
		t.Fatalf("consume write fail: %v", err)
	}

	consResp, err := protocol.ReadResponse(followerConn, true)
	if err != nil {
		t.Fatalf("consume read resp fail: %v", err)
	}
	if consResp.Status != protocol.StatusOk {
		t.Fatalf("consume error: %s", consResp.ErrMsg)
	}

	if len(consResp.Records) != 1 {
		t.Fatalf("expected 1 replicated record, got %d", len(consResp.Records))
	}
	rec := consResp.Records[0]
	if !bytes.Equal(rec.Key, key) || !bytes.Equal(rec.Value, value) {
		t.Errorf("replicated record content mismatch")
	}
}
