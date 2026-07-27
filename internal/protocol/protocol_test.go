package protocol

import (
	"bytes"
	"testing"
)

func TestProduceRequestRoundTrip(t *testing.T) {
	req := &Request{
		Type:  ReqProduce,
		Topic: "test-topic",
		Key:   []byte("some-key"),
		Value: []byte("some-value"),
	}

	var buf bytes.Buffer
	if err := WriteRequest(&buf, req); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	readReq, err := ReadRequest(&buf)
	if err != nil {
		t.Fatalf("failed to read request: %v", err)
	}

	if readReq.Type != req.Type {
		t.Errorf("type mismatch: %d vs %d", readReq.Type, req.Type)
	}
	if readReq.Topic != req.Topic {
		t.Errorf("topic mismatch: %s vs %s", readReq.Topic, req.Topic)
	}
	if !bytes.Equal(readReq.Key, req.Key) {
		t.Errorf("key mismatch: %s vs %s", readReq.Key, req.Key)
	}
	if !bytes.Equal(readReq.Value, req.Value) {
		t.Errorf("value mismatch: %s vs %s", readReq.Value, req.Value)
	}
}

func TestConsumeRequestRoundTrip(t *testing.T) {
	req := &Request{
		Type:     ReqConsume,
		Topic:    "another-topic",
		Offset:   4123,
		MaxBytes: 1048576,
	}

	var buf bytes.Buffer
	if err := WriteRequest(&buf, req); err != nil {
		t.Fatalf("failed to write request: %v", err)
	}

	readReq, err := ReadRequest(&buf)
	if err != nil {
		t.Fatalf("failed to read request: %v", err)
	}

	if readReq.Type != req.Type {
		t.Errorf("type mismatch: %d vs %d", readReq.Type, req.Type)
	}
	if readReq.Topic != req.Topic {
		t.Errorf("topic mismatch: %s vs %s", readReq.Topic, req.Topic)
	}
	if readReq.Offset != req.Offset {
		t.Errorf("offset mismatch: %d vs %d", readReq.Offset, req.Offset)
	}
	if readReq.MaxBytes != req.MaxBytes {
		t.Errorf("maxBytes mismatch: %d vs %d", readReq.MaxBytes, req.MaxBytes)
	}
}

func TestResponseRoundTrip(t *testing.T) {
	resp1 := &Response{
		Status: StatusOk,
		Offset: 9988,
	}

	var buf1 bytes.Buffer
	if err := WriteResponse(&buf1, resp1); err != nil {
		t.Fatalf("failed to write response 1: %v", err)
	}

	readResp1, err := ReadResponse(&buf1, false)
	if err != nil {
		t.Fatalf("failed to read response 1: %v", err)
	}
	if readResp1.Status != resp1.Status || readResp1.Offset != resp1.Offset {
		t.Errorf("resp 1 mismatch")
	}

	resp2 := &Response{
		Status: StatusOk,
		Offset: 12345,
		Records: []Record{
			{Offset: 100, Key: []byte("k1"), Value: []byte("v1")},
			{Offset: 110, Key: []byte("k2"), Value: []byte("v2")},
		},
	}

	var buf2 bytes.Buffer
	if err := WriteResponse(&buf2, resp2); err != nil {
		t.Fatalf("failed to write response 2: %v", err)
	}

	readResp2, err := ReadResponse(&buf2, true)
	if err != nil {
		t.Fatalf("failed to read response 2: %v", err)
	}
	if readResp2.Status != resp2.Status || readResp2.Offset != resp2.Offset {
		t.Errorf("resp 2 status/offset mismatch")
	}
	if len(readResp2.Records) != len(resp2.Records) {
		t.Errorf("records length mismatch: %d vs %d", len(readResp2.Records), len(resp2.Records))
	}
	for i := range resp2.Records {
		if readResp2.Records[i].Offset != resp2.Records[i].Offset ||
			!bytes.Equal(readResp2.Records[i].Key, resp2.Records[i].Key) ||
			!bytes.Equal(readResp2.Records[i].Value, resp2.Records[i].Value) {
			t.Errorf("record %d mismatch", i)
		}
	}

	resp3 := &Response{
		Status: StatusErr,
		ErrMsg: "something went wrong",
	}

	var buf3 bytes.Buffer
	if err := WriteResponse(&buf3, resp3); err != nil {
		t.Fatalf("failed to write response 3: %v", err)
	}

	readResp3, err := ReadResponse(&buf3, false)
	if err != nil {
		t.Fatalf("failed to read response 3: %v", err)
	}
	if readResp3.Status != resp3.Status || readResp3.ErrMsg != resp3.ErrMsg {
		t.Errorf("error response mismatch: status %d vs %d, msg %s vs %s", readResp3.Status, resp3.Status, readResp3.ErrMsg, resp3.ErrMsg)
	}
}

func TestOffsetCommitRequestRoundTrip(t *testing.T) {
	req := &Request{
		Type:      ReqOffsetCommit,
		GroupID:   "group-a",
		Topic:     "topic-a",
		Partition: 2,
		Offset:    5566,
	}

	var buf bytes.Buffer
	if err := WriteRequest(&buf, req); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	readReq, err := ReadRequest(&buf)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if readReq.Type != req.Type || readReq.GroupID != req.GroupID ||
		readReq.Topic != req.Topic || readReq.Partition != req.Partition ||
		readReq.Offset != req.Offset {
		t.Errorf("offset commit mismatch")
	}
}

func TestOffsetFetchRequestRoundTrip(t *testing.T) {
	req := &Request{
		Type:      ReqOffsetFetch,
		GroupID:   "group-b",
		Topic:     "topic-b",
		Partition: 4,
	}

	var buf bytes.Buffer
	if err := WriteRequest(&buf, req); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	readReq, err := ReadRequest(&buf)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if readReq.Type != req.Type || readReq.GroupID != req.GroupID ||
		readReq.Topic != req.Topic || readReq.Partition != req.Partition {
		t.Errorf("offset fetch mismatch")
	}
}

func TestJoinGroupRequestRoundTrip(t *testing.T) {
	req := &Request{
		Type:     ReqJoinGroup,
		GroupID:  "group-c",
		MemberID: "member-1",
		Topics:   []string{"topic-1", "topic-2"},
	}

	var buf bytes.Buffer
	if err := WriteRequest(&buf, req); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	readReq, err := ReadRequest(&buf)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if readReq.Type != req.Type || readReq.GroupID != req.GroupID ||
		readReq.MemberID != req.MemberID || len(readReq.Topics) != 2 ||
		readReq.Topics[0] != "topic-1" || readReq.Topics[1] != "topic-2" {
		t.Errorf("join group mismatch")
	}
}

func TestGroupResponseRoundTrip(t *testing.T) {
	resp := &Response{
		Status:     StatusOk,
		Generation: 5,
		Assignment: map[string][]int32{
			"topic-1": {0, 1},
		},
	}

	var buf bytes.Buffer
	if err := WriteResponse(&buf, resp); err != nil {
		t.Fatalf("failed to write: %v", err)
	}

	readResp, err := ReadGroupResponse(&buf)
	if err != nil {
		t.Fatalf("failed to read: %v", err)
	}

	if readResp.Status != resp.Status || readResp.Generation != resp.Generation {
		t.Errorf("group response metadata mismatch")
	}

	parts := readResp.Assignment["topic-1"]
	if len(parts) != 2 || parts[0] != 0 || parts[1] != 1 {
		t.Errorf("group response assignment mismatch")
	}
}
