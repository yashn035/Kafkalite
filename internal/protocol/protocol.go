package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	ReqProduce      byte = 1
	ReqConsume      byte = 2
	ReqJoinReplica  byte = 3
	ReqReplicate    byte = 4
	ReqOffsetCommit byte = 5
	ReqOffsetFetch  byte = 6
	ReqJoinGroup    byte = 10
	ReqSyncGroup    byte = 11

	StatusOk  byte = 0
	StatusErr byte = 1
)

type Request struct {
	Type      byte
	Topic     string
	Offset    int64
	Key       []byte
	Value     []byte
	MaxBytes  int32
	GroupID   string
	Partition int32
	MemberID  string
	Topics    []string
}

type Record struct {
	Offset int64
	Key    []byte
	Value  []byte
}

type Response struct {
	Status     byte
	Offset     int64
	Records    []Record
	ErrMsg     string
	Generation int
	Assignment map[string][]int32
}

func getRequestPayloadSize(req *Request, topicLen int) int32 {
	if req.Type == ReqProduce {
		return 1 + 2 + int32(topicLen) + 4 + int32(len(req.Key)) + 4 + int32(len(req.Value))
	}
	if req.Type == ReqReplicate {
		return 1 + 2 + int32(topicLen) + 8 + 4 + int32(len(req.Key)) + 4 + int32(len(req.Value))
	}
	if req.Type == ReqJoinReplica {
		return 1 + 2 + int32(topicLen)
	}
	if req.Type == ReqOffsetCommit {
		return 1 + 2 + int32(len(req.GroupID)) + 2 + int32(topicLen) + 4 + 8
	}
	if req.Type == ReqOffsetFetch {
		return 1 + 2 + int32(len(req.GroupID)) + 2 + int32(topicLen) + 4
	}
	if req.Type == ReqJoinGroup {
		size := 1 + 2 + int32(len(req.GroupID)) + 2 + int32(len(req.MemberID)) + 2
		for _, t := range req.Topics {
			size += 2 + int32(len(t))
		}
		return size
	}
	if req.Type == ReqSyncGroup {
		return 1 + 2 + int32(len(req.GroupID)) + 2 + int32(len(req.MemberID))
	}
	return 1 + 2 + int32(topicLen) + 8 + 4
}

func writeProducePayload(w io.Writer, key, value []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(key))); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := w.Write(value)
	return err
}

func writeConsumePayload(w io.Writer, offset int64, maxBytes int32) error {
	if err := binary.Write(w, binary.BigEndian, offset); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, maxBytes)
}

func writeReplicatePayload(w io.Writer, req *Request) error {
	if err := binary.Write(w, binary.BigEndian, req.Offset); err != nil {
		return err
	}
	return writeProducePayload(w, req.Key, req.Value)
}

func writeOffsetCommitPayload(w io.Writer, req *Request) error {
	gBytes := []byte(req.GroupID)
	if err := binary.Write(w, binary.BigEndian, uint16(len(gBytes))); err != nil {
		return err
	}
	if _, err := w.Write(gBytes); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, req.Partition); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, req.Offset)
}

func writeOffsetFetchPayload(w io.Writer, req *Request) error {
	gBytes := []byte(req.GroupID)
	if err := binary.Write(w, binary.BigEndian, uint16(len(gBytes))); err != nil {
		return err
	}
	if _, err := w.Write(gBytes); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, req.Partition)
}

func writeJoinGroupPayload(w io.Writer, req *Request) error {
	gBytes := []byte(req.GroupID)
	if err := binary.Write(w, binary.BigEndian, uint16(len(gBytes))); err != nil {
		return err
	}
	w.Write(gBytes)
	mBytes := []byte(req.MemberID)
	if err := binary.Write(w, binary.BigEndian, uint16(len(mBytes))); err != nil {
		return err
	}
	w.Write(mBytes)
	if err := binary.Write(w, binary.BigEndian, uint16(len(req.Topics))); err != nil {
		return err
	}
	for _, t := range req.Topics {
		tBytes := []byte(t)
		binary.Write(w, binary.BigEndian, uint16(len(tBytes)))
		w.Write(tBytes)
	}
	return nil
}

func writeSyncGroupPayload(w io.Writer, req *Request) error {
	gBytes := []byte(req.GroupID)
	if err := binary.Write(w, binary.BigEndian, uint16(len(gBytes))); err != nil {
		return err
	}
	w.Write(gBytes)
	mBytes := []byte(req.MemberID)
	if err := binary.Write(w, binary.BigEndian, uint16(len(mBytes))); err != nil {
		return err
	}
	_, err := w.Write(mBytes)
	return err
}

func WriteRequest(w io.Writer, req *Request) error {
	topicBytes := []byte(req.Topic)
	payloadSize := getRequestPayloadSize(req, len(topicBytes))

	if err := binary.Write(w, binary.BigEndian, payloadSize); err != nil {
		return err
	}
	if _, err := w.Write([]byte{req.Type}); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint16(len(topicBytes))); err != nil {
		return err
	}
	if _, err := w.Write(topicBytes); err != nil {
		return err
	}

	if req.Type == ReqProduce {
		return writeProducePayload(w, req.Key, req.Value)
	} else if req.Type == ReqReplicate {
		return writeReplicatePayload(w, req)
	} else if req.Type == ReqJoinReplica {
		return nil
	} else if req.Type == ReqOffsetCommit {
		return writeOffsetCommitPayload(w, req)
	} else if req.Type == ReqOffsetFetch {
		return writeOffsetFetchPayload(w, req)
	} else if req.Type == ReqJoinGroup {
		return writeJoinGroupPayload(w, req)
	} else if req.Type == ReqSyncGroup {
		return writeSyncGroupPayload(w, req)
	}
	return writeConsumePayload(w, req.Offset, req.MaxBytes)
}

func readString16(r io.Reader) (string, error) {
	var length uint16
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return "", err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func readProducePayload(r io.Reader, req *Request) (*Request, error) {
	var keyLen uint32
	if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
		return nil, err
	}
	req.Key = make([]byte, keyLen)
	if _, err := io.ReadFull(r, req.Key); err != nil {
		return nil, err
	}

	var valLen uint32
	if err := binary.Read(r, binary.BigEndian, &valLen); err != nil {
		return nil, err
	}
	req.Value = make([]byte, valLen)
	if _, err := io.ReadFull(r, req.Value); err != nil {
		return nil, err
	}
	return req, nil
}

func readConsumePayload(r io.Reader, req *Request) (*Request, error) {
	if err := binary.Read(r, binary.BigEndian, &req.Offset); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &req.MaxBytes); err != nil {
		return nil, err
	}
	return req, nil
}

func readReplicatePayload(r io.Reader, req *Request) (*Request, error) {
	if err := binary.Read(r, binary.BigEndian, &req.Offset); err != nil {
		return nil, err
	}
	return readProducePayload(r, req)
}

func readOffsetCommitPayload(r io.Reader, req *Request) (*Request, error) {
	gID, err := readString16(r)
	if err != nil {
		return nil, err
	}
	req.GroupID = gID
	if err := binary.Read(r, binary.BigEndian, &req.Partition); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &req.Offset); err != nil {
		return nil, err
	}
	return req, nil
}

func readOffsetFetchPayload(r io.Reader, req *Request) (*Request, error) {
	gID, err := readString16(r)
	if err != nil {
		return nil, err
	}
	req.GroupID = gID
	if err := binary.Read(r, binary.BigEndian, &req.Partition); err != nil {
		return nil, err
	}
	return req, nil
}

func readJoinGroupPayload(r io.Reader, req *Request) (*Request, error) {
	gID, err := readString16(r)
	if err != nil {
		return nil, err
	}
	req.GroupID = gID

	mID, err := readString16(r)
	if err != nil {
		return nil, err
	}
	req.MemberID = mID

	var tCount uint16
	if err := binary.Read(r, binary.BigEndian, &tCount); err != nil {
		return nil, err
	}
	req.Topics = make([]string, tCount)
	for i := uint16(0); i < tCount; i++ {
		tName, err := readString16(r)
		if err != nil {
			return nil, err
		}
		req.Topics[i] = tName
	}
	return req, nil
}

func readSyncGroupPayload(r io.Reader, req *Request) (*Request, error) {
	gID, err := readString16(r)
	if err != nil {
		return nil, err
	}
	req.GroupID = gID

	mID, err := readString16(r)
	if err != nil {
		return nil, err
	}
	req.MemberID = mID
	return req, nil
}

func ReadRequest(r io.Reader) (*Request, error) {
	var totalLen int32
	if err := binary.Read(r, binary.BigEndian, &totalLen); err != nil {
		return nil, err
	}

	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, typeBuf); err != nil {
		return nil, err
	}
	reqType := typeBuf[0]

	var topicLen uint16
	if err := binary.Read(r, binary.BigEndian, &topicLen); err != nil {
		return nil, err
	}

	topicBuf := make([]byte, topicLen)
	if _, err := io.ReadFull(r, topicBuf); err != nil {
		return nil, err
	}

	req := &Request{Type: reqType, Topic: string(topicBuf)}
	if reqType == ReqProduce {
		return readProducePayload(r, req)
	} else if reqType == ReqConsume {
		return readConsumePayload(r, req)
	} else if reqType == ReqReplicate {
		return readReplicatePayload(r, req)
	} else if reqType == ReqJoinReplica {
		return req, nil
	} else if reqType == ReqOffsetCommit {
		return readOffsetCommitPayload(r, req)
	} else if reqType == ReqOffsetFetch {
		return readOffsetFetchPayload(r, req)
	} else if reqType == ReqJoinGroup {
		return readJoinGroupPayload(r, req)
	} else if reqType == ReqSyncGroup {
		return readSyncGroupPayload(r, req)
	}
	return nil, errors.New("unknown request type")
}

func getResponsePayloadSize(resp *Response) int32 {
	if resp.Status == StatusErr {
		return 1 + 2 + int32(len(resp.ErrMsg))
	}
	if resp.Assignment != nil {
		size := int32(1 + 4 + 2)
		for topic, parts := range resp.Assignment {
			size += 2 + int32(len(topic)) + 2 + int32(len(parts))*4
		}
		return size
	}
	size := int32(1 + 8)
	if resp.Records != nil {
		size += 4
		for _, rec := range resp.Records {
			size += 8 + 4 + int32(len(rec.Key)) + 4 + int32(len(rec.Value))
		}
	}
	return size
}

func writeRecord(w io.Writer, rec *Record) error {
	if err := binary.Write(w, binary.BigEndian, rec.Offset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(rec.Key))); err != nil {
		return err
	}
	if _, err := w.Write(rec.Key); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(rec.Value))); err != nil {
		return err
	}
	_, err := w.Write(rec.Value)
	return err
}

func writeAssignmentPayload(w io.Writer, assignment map[string][]int32) error {
	if err := binary.Write(w, binary.BigEndian, uint16(len(assignment))); err != nil {
		return err
	}
	for topic, parts := range assignment {
		tBytes := []byte(topic)
		if err := binary.Write(w, binary.BigEndian, uint16(len(tBytes))); err != nil {
			return err
		}
		w.Write(tBytes)
		if err := binary.Write(w, binary.BigEndian, uint16(len(parts))); err != nil {
			return err
		}
		for _, p := range parts {
			binary.Write(w, binary.BigEndian, p)
		}
	}
	return nil
}

func writeOkResponsePayload(w io.Writer, resp *Response) error {
	if resp.Assignment != nil {
		if err := binary.Write(w, binary.BigEndian, int32(resp.Generation)); err != nil {
			return err
		}
		return writeAssignmentPayload(w, resp.Assignment)
	}
	if err := binary.Write(w, binary.BigEndian, resp.Offset); err != nil {
		return err
	}
	if resp.Records == nil {
		return nil
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(resp.Records))); err != nil {
		return err
	}
	for _, rec := range resp.Records {
		if err := writeRecord(w, &rec); err != nil {
			return err
		}
	}
	return nil
}

func WriteResponse(w io.Writer, resp *Response) error {
	payloadSize := getResponsePayloadSize(resp)
	if err := binary.Write(w, binary.BigEndian, payloadSize); err != nil {
		return err
	}
	if _, err := w.Write([]byte{resp.Status}); err != nil {
		return err
	}
	if resp.Status == StatusErr {
		errMsgBytes := []byte(resp.ErrMsg)
		if err := binary.Write(w, binary.BigEndian, uint16(len(errMsgBytes))); err != nil {
			return err
		}
		_, err := w.Write(errMsgBytes)
		return err
	}
	return writeOkResponsePayload(w, resp)
}

func readRecord(r io.Reader) (*Record, error) {
	var rec Record
	if err := binary.Read(r, binary.BigEndian, &rec.Offset); err != nil {
		return nil, err
	}
	var kLen uint32
	if err := binary.Read(r, binary.BigEndian, &kLen); err != nil {
		return nil, err
	}
	rec.Key = make([]byte, kLen)
	if _, err := io.ReadFull(r, rec.Key); err != nil {
		return nil, err
	}
	var vLen uint32
	if err := binary.Read(r, binary.BigEndian, &vLen); err != nil {
		return nil, err
	}
	rec.Value = make([]byte, vLen)
	if _, err := io.ReadFull(r, rec.Value); err != nil {
		return nil, err
	}
	return &rec, nil
}

func readOkResponsePayload(r io.Reader, resp *Response, isConsume bool) (*Response, error) {
	if err := binary.Read(r, binary.BigEndian, &resp.Offset); err != nil {
		return nil, err
	}
	if !isConsume {
		return resp, nil
	}

	var numRecs uint32
	if err := binary.Read(r, binary.BigEndian, &numRecs); err != nil {
		return nil, err
	}

	resp.Records = make([]Record, numRecs)
	for i := uint32(0); i < numRecs; i++ {
		rec, err := readRecord(r)
		if err != nil {
			return nil, err
		}
		resp.Records[i] = *rec
	}
	return resp, nil
}

func ReadResponse(r io.Reader, isConsume bool) (*Response, error) {
	var totalLen int32
	if err := binary.Read(r, binary.BigEndian, &totalLen); err != nil {
		return nil, err
	}

	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, statusBuf); err != nil {
		return nil, err
	}
	status := statusBuf[0]

	resp := &Response{Status: status}
	if status == StatusErr {
		var errLen uint16
		if err := binary.Read(r, binary.BigEndian, &errLen); err != nil {
			return nil, err
		}
		errMsgBuf := make([]byte, errLen)
		if _, err := io.ReadFull(r, errMsgBuf); err != nil {
			return nil, err
		}
		resp.ErrMsg = string(errMsgBuf)
		return resp, nil
	}

	return readOkResponsePayload(r, resp, isConsume)
}

func readAssignmentPayload(r io.Reader) (map[string][]int32, error) {
	var mSize uint16
	if err := binary.Read(r, binary.BigEndian, &mSize); err != nil {
		return nil, err
	}
	assignment := make(map[string][]int32, mSize)
	for i := uint16(0); i < mSize; i++ {
		tName, err := readString16(r)
		if err != nil {
			return nil, err
		}
		var pCount uint16
		if err := binary.Read(r, binary.BigEndian, &pCount); err != nil {
			return nil, err
		}
		parts := make([]int32, pCount)
		for j := uint16(0); j < pCount; j++ {
			if err := binary.Read(r, binary.BigEndian, &parts[j]); err != nil {
				return nil, err
			}
		}
		assignment[tName] = parts
	}
	return assignment, nil
}

func ReadGroupResponse(r io.Reader) (*Response, error) {
	var totalLen int32
	if err := binary.Read(r, binary.BigEndian, &totalLen); err != nil {
		return nil, err
	}
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, statusBuf); err != nil {
		return nil, err
	}
	status := statusBuf[0]
	resp := &Response{Status: status}
	if status == StatusErr {
		var errLen uint16
		if err := binary.Read(r, binary.BigEndian, &errLen); err != nil {
			return nil, err
		}
		errMsgBuf := make([]byte, errLen)
		if _, err := io.ReadFull(r, errMsgBuf); err != nil {
			return nil, err
		}
		resp.ErrMsg = string(errMsgBuf)
		return resp, nil
	}
	var gen int32
	if err := binary.Read(r, binary.BigEndian, &gen); err != nil {
		return nil, err
	}
	resp.Generation = int(gen)
	ass, err := readAssignmentPayload(r)
	if err != nil {
		return nil, err
	}
	resp.Assignment = ass
	return resp, nil
}
