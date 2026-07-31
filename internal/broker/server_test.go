package broker

import (
	"bytes"
	"net"
	"os"
	"testing"
	"time"

	"kafkalite/internal/protocol"
)

func TestServerProduceAndConsume(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "kafkalite-broker-test-*")
	if err != nil {
		t.Fatalf("failed temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	srv, err := NewServer(0, tmpDir, 1, 50*time.Millisecond, 1024, 30*time.Second, 10000)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	defer srv.Close()

	addr := "127.0.0.1:0"
	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen error: %v", err)
	}
	freeAddr := l.Addr().String()
	l.Close()

	go func() {
		if err := srv.Start(freeAddr); err != nil {
			// Server closed is expected
		}
	}()

	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", freeAddr)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	topic := "orders"
	key := []byte("key1")
	val := []byte("val1")

	req := &protocol.Request{
		Type:  protocol.ReqProduce,
		Topic: topic,
		Key:   key,
		Value: val,
	}
	if err := protocol.WriteRequest(conn, req); err != nil {
		t.Fatalf("failed write request: %v", err)
	}

	resp, err := protocol.ReadResponse(conn, false)
	if err != nil {
		t.Fatalf("failed read response: %v", err)
	}
	if resp.Status != protocol.StatusOk {
		t.Fatalf("resp status error: %s", resp.ErrMsg)
	}
	producedOffset := resp.Offset
	if producedOffset != 0 {
		t.Errorf("expected offset 0, got %d", producedOffset)
	}

	time.Sleep(200 * time.Millisecond)

	consReq := &protocol.Request{
		Type:     protocol.ReqConsume,
		Topic:    topic,
		Offset:   0,
		MaxBytes: 1024,
	}
	if err := protocol.WriteRequest(conn, consReq); err != nil {
		t.Fatalf("failed write consume request: %v", err)
	}

	consResp, err := protocol.ReadResponse(conn, true)
	if err != nil {
		t.Fatalf("failed read consume response: %v", err)
	}
	if consResp.Status != protocol.StatusOk {
		t.Fatalf("consume response status error: %s", consResp.ErrMsg)
	}
	if len(consResp.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(consResp.Records))
	}
	rec := consResp.Records[0]
	if rec.Offset != producedOffset || !bytes.Equal(rec.Key, key) || !bytes.Equal(rec.Value, val) {
		t.Errorf("record mismatch")
	}
}
