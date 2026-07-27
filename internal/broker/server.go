package broker

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"kafkalite/internal/consumer"
	"kafkalite/internal/protocol"
	"kafkalite/internal/storage"
	"kafkalite/pkg/metrics"
)

type Server struct {
	mu          sync.Mutex
	brokerID    int
	dataDir     string
	listener    net.Listener
	topics      map[string]*storage.Segment
	shutdown    chan struct{}
	repManager  *ReplicationManager
	offsetStore *consumer.OffsetStore
	controller  *Controller
	coordinator *consumer.GroupCoordinator
}

func NewServer(brokerID int, dataDir string) (*Server, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	store, err := consumer.NewOffsetStore(dataDir)
	if err != nil {
		return nil, err
	}
	return &Server{
		brokerID:    brokerID,
		dataDir:     dataDir,
		topics:      make(map[string]*storage.Segment),
		shutdown:    make(chan struct{}),
		repManager:  NewReplicationManager(),
		offsetStore: store,
		controller:  NewController(brokerID, dataDir),
		coordinator: consumer.NewGroupCoordinator(),
	}, nil
}

func (s *Server) Start(addr string) error {
	s.controller.Start()
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-s.shutdown:
				return nil
			default:
				return err
			}
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	close(s.shutdown)
	s.controller.Close()
	var err error
	if s.listener != nil {
		err = s.listener.Close()
	}
	for _, seg := range s.topics {
		seg.Close()
	}
	return err
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	for {
		select {
		case <-s.shutdown:
			return
		default:
		}
		req, err := protocol.ReadRequest(conn)
		if err != nil {
			if err != io.EOF {
				log.Printf("ReadRequest error: %v", err)
			}
			return
		}
		if req.Type == protocol.ReqJoinReplica {
			s.repManager.RegisterReplica(req.Topic, conn)
			return
		}
		resp := s.processRequest(req)
		if err := protocol.WriteResponse(conn, resp); err != nil {
			log.Printf("WriteResponse error: %v", err)
			return
		}
	}
}

func (s *Server) getOrCreateTopicSegment(topic string) (*storage.Segment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if seg, ok := s.topics[topic]; ok {
		return seg, nil
	}

	path := filepath.Join(s.dataDir, fmt.Sprintf("%s.log", topic))
	seg, err := storage.NewSegment(path)
	if err != nil {
		return nil, err
	}
	s.topics[topic] = seg
	return seg, nil
}

func (s *Server) processOffsetRequest(req *protocol.Request) *protocol.Response {
	if req.Type == protocol.ReqOffsetCommit {
		err := s.offsetStore.CommitOffset(req.GroupID, req.Topic, req.Partition, req.Offset)
		if err != nil {
			return &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
		}
		return &protocol.Response{Status: protocol.StatusOk}
	}
	off, err := s.offsetStore.FetchOffset(req.GroupID, req.Topic, req.Partition)
	if err != nil {
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
	}
	return &protocol.Response{Status: protocol.StatusOk, Offset: off}
}

func (s *Server) handleProduceRequest(req *protocol.Request, seg *storage.Segment) *protocol.Response {
	start := time.Now()
	off, err := seg.Append(req.Key, req.Value)
	if err != nil {
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
	}
	if err := s.repManager.Replicate(req.Topic, off, req.Key, req.Value); err != nil {
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
	}
	metrics.RecordProduce(req.Topic, seg.Size(), time.Since(start))
	return &protocol.Response{Status: protocol.StatusOk, Offset: off}
}

func (s *Server) handleConsumeRequest(req *protocol.Request, seg *storage.Segment) *protocol.Response {
	records, nextOffset, err := seg.ReadRecords(req.Offset, int(req.MaxBytes))
	if err != nil {
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
	}
	respRecs := make([]protocol.Record, len(records))
	for i, r := range records {
		respRecs[i] = protocol.Record{Offset: r.Offset, Key: r.Key, Value: r.Value}
	}
	metrics.RecordConsume(req.Topic, len(records))
	return &protocol.Response{Status: protocol.StatusOk, Offset: nextOffset, Records: respRecs}
}

func (s *Server) processGroupRequest(req *protocol.Request) *protocol.Response {
	if req.Type == protocol.ReqJoinGroup {
		gen := s.coordinator.JoinGroup(req.GroupID, req.MemberID, req.Topics)
		return &protocol.Response{
			Status:     protocol.StatusOk,
			Generation: gen,
		}
	}
	ass := s.coordinator.SyncGroup(req.GroupID, req.MemberID)
	return &protocol.Response{
		Status:     protocol.StatusOk,
		Assignment: ass,
	}
}

func parseTopicPartition(topic string) (string, int32) {
	if idx := strings.LastIndex(topic, "-"); idx != -1 {
		partStr := topic[idx+1:]
		if val, err := strconv.Atoi(partStr); err == nil {
			return topic[:idx], int32(val)
		}
	}
	return topic, 0
}

func (s *Server) isLeaderFor(topic string) bool {
	baseTopic, part := parseTopicPartition(topic)
	leaderID := s.controller.GetLeader(baseTopic, part)
	return leaderID == s.brokerID
}

func (s *Server) processRequest(req *protocol.Request) *protocol.Response {
	if req.Type == protocol.ReqOffsetCommit || req.Type == protocol.ReqOffsetFetch {
		return s.processOffsetRequest(req)
	}
	if req.Type == protocol.ReqJoinGroup || req.Type == protocol.ReqSyncGroup {
		return s.processGroupRequest(req)
	}
	if !s.isLeaderFor(req.Topic) {
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: "Not Leader"}
	}

	seg, err := s.getOrCreateTopicSegment(req.Topic)
	if err != nil {
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
	}

	if req.Type == protocol.ReqProduce {
		return s.handleProduceRequest(req, seg)
	} else if req.Type == protocol.ReqConsume {
		return s.handleConsumeRequest(req, seg)
	}
	return &protocol.Response{Status: protocol.StatusErr, ErrMsg: "invalid request type"}
}

func (s *Server) GetLedPartitions() []string {
	return s.controller.GetLedPartitions(s.brokerID)
}
