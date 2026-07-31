package broker

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"kafkalite/internal/auth"
	"kafkalite/internal/consumer"
	"kafkalite/internal/producer"
	"kafkalite/internal/protocol"
	"kafkalite/internal/schema"
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
	coordinator    *consumer.GroupCoordinator
	producerSM     *producer.IdempotentManager
	deduplicator   *consumer.Deduplicator
	schemaRegistry *schema.Registry
	rebalancer     *Rebalancer
	acks           int
	flushInt    time.Duration
	batchSize   int
}

func NewServer(brokerID int, dataDir string, acks int, flushInt time.Duration, batchSize int, rebInterval time.Duration, rebThreshold int64) (*Server, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	store, err := consumer.NewOffsetStore(dataDir)
	if err != nil {
		return nil, err
	}
	controller := NewController(brokerID, dataDir)
	return &Server{
		brokerID:       brokerID,
		dataDir:        dataDir,
		topics:         make(map[string]*storage.Segment),
		shutdown:       make(chan struct{}),
		repManager:     NewReplicationManager(),
		offsetStore:    store,
		controller:     controller,
		coordinator:    consumer.NewGroupCoordinator(),
		producerSM:     producer.NewIdempotentManager(24 * time.Hour),
		deduplicator:   consumer.NewDeduplicator(dataDir),
		schemaRegistry: schema.NewRegistry(),
		rebalancer:     NewRebalancer(controller, rebInterval, rebThreshold),
		acks:           acks,
		flushInt:       flushInt,
		batchSize:      batchSize,
	}, nil
}

func (s *Server) Start(addr string) error {
	s.controller.Start()
	s.rebalancer.Start()
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
	s.rebalancer.Stop()
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
	isReplica := false
	defer func() {
		if !isReplica {
			conn.Close()
		}
	}()

	role, token, err := auth.AuthenticateConnection(conn)
	if err != nil {
		protocol.WriteResponse(conn, &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()})
		return
	}
	if err := protocol.WriteResponse(conn, &protocol.Response{Status: protocol.StatusOk, Token: token}); err != nil {
		return
	}

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
			isReplica = true
			s.repManager.RegisterReplica(req.Topic, conn)
			return
		}
		resp := s.processRequest(req, role)
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
	seg, err := storage.NewSegment(path, s.flushInt, s.batchSize)
	if err != nil {
		return nil, err
	}
	s.topics[topic] = seg
	return seg, nil
}

func (s *Server) processOffsetRequest(req *protocol.Request) *protocol.Response {
	if req.Type == protocol.ReqOffsetCommit {
		if !req.Success {
			seg, err := s.getOrCreateTopicSegment(req.Topic)
			if err == nil {
				recs, _, _ := seg.ReadRecords(req.Offset, 1024)
				if len(recs) > 0 {
					consumer.GlobalRetryManager.HandleFailure(req.GroupID, req.Topic, req.Partition, req.Offset, recs[0].Key, recs[0].Value)
				}
			}
		}
		if req.ProcessedIDs != nil {
			s.deduplicator.MarkProcessed(req.GroupID, req.ProcessedIDs)
		}
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

	// Feature 3: Schema Validation
	if err := s.schemaRegistry.Validate(req.Topic, req.Value); err != nil {
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: "ErrInvalidSchema: " + err.Error()}
	}

	// Check Idempotency
	if req.ProducerID != 0 && req.MessageID != "" {
		if cachedOffset, exists := s.producerSM.CheckDuplicate(req.ProducerID, req.MessageID); exists {
			return &protocol.Response{Status: protocol.StatusOk, Offset: cachedOffset}
		}
	}

	off, err := seg.AppendBatch(req.Key, req.Value)
	if err != nil {
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
	}

	if req.ProducerID != 0 && req.MessageID != "" {
		s.producerSM.RecordMessage(req.ProducerID, req.MessageID, off)
	}

	switch s.acks {
	case 0:
		go s.repManager.Replicate(req.Topic, off, req.Key, req.Value)
		return &protocol.Response{Status: protocol.StatusOk, Offset: off}
	case 1:
		go s.repManager.Replicate(req.Topic, off, req.Key, req.Value)
	default:
		// acks == -1 (block for replication)
		if err := s.repManager.Replicate(req.Topic, off, req.Key, req.Value); err != nil {
			return &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
		}
	}

	metrics.RecordProduce(req.Topic, seg.Size(), time.Since(start))
	return &protocol.Response{Status: protocol.StatusOk, Offset: off}
}

func (s *Server) handleConsumeRequest(req *protocol.Request, seg *storage.Segment) *protocol.Response {
	records, nextOffset, err := seg.ReadRecords(req.Offset, int(req.MaxBytes))
	if err != nil {
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
	}
	respRecs := make([]protocol.Record, 0, len(records))
	for _, r := range records {
		// Feature 2: Filter by StartTime/EndTime
		if req.StartTime > 0 && r.Timestamp < req.StartTime {
			continue
		}
		if req.EndTime > 0 && r.Timestamp > req.EndTime {
			continue
		}
		respRecs = append(respRecs, protocol.Record{Timestamp: r.Timestamp, Offset: r.Offset, Key: r.Key, Value: r.Value})
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
			Assignment: make(map[string][]int32),
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

func (s *Server) processRequest(req *protocol.Request, role string) *protocol.Response {
	if req.Type == protocol.ReqOffsetCommit || req.Type == protocol.ReqOffsetFetch {
		return s.processOffsetRequest(req)
	}
	if req.Type == protocol.ReqJoinGroup || req.Type == protocol.ReqSyncGroup {
		return s.processGroupRequest(req)
	}
	if req.Type == protocol.ReqRegisterSchema {
		err := s.schemaRegistry.Register(req.Topic, req.SchemaDef)
		if err != nil {
			return &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
		}
		return &protocol.Response{Status: protocol.StatusOk}
	}
	if !s.isLeaderFor(req.Topic) {
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: "Not Leader"}
	}

	seg, err := s.getOrCreateTopicSegment(req.Topic)
	if err != nil {
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: err.Error()}
	}

	switch req.Type {
	case protocol.ReqProduce:
		if !auth.CanProduce(role) {
			return &protocol.Response{Status: protocol.StatusErr, ErrMsg: "permission denied"}
		}
		return s.handleProduceRequest(req, seg)
	case protocol.ReqConsume:
		if !auth.CanConsume(role) {
			return &protocol.Response{Status: protocol.StatusErr, ErrMsg: "permission denied"}
		}
		return s.handleConsumeRequest(req, seg)
	default:
		return &protocol.Response{Status: protocol.StatusErr, ErrMsg: "invalid request type"}
	}
}

func (s *Server) GetLedPartitions() []string {
	return s.controller.GetLedPartitions(s.brokerID)
}
