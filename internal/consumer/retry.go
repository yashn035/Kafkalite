package consumer

import (
	"fmt"
	"log"
	"net"
	"time"

	"kafkalite/internal/protocol"
)

type RetryManager struct {
	maxRetries int
	failures   map[string]int
}

var GlobalRetryManager = &RetryManager{
	maxRetries: 3,
	failures:   make(map[string]int),
}

func (rm *RetryManager) HandleFailure(group, topic string, partition int32, offset int64, key, value []byte) {
	k := fmt.Sprintf("%s-%s-%d-%d", group, topic, partition, offset)
	rm.failures[k]++
	retries := rm.failures[k]

	if retries >= rm.maxRetries {
		SendToDLQ(topic, partition, key, value, "max retries exceeded")
		return
	}

	backoff := time.Duration(1<<retries) * time.Second
	log.Printf("Scheduling retry for %s in %v", k, backoff)

	go func(t string, p int32, kBytes, vBytes []byte, delay time.Duration) {
		time.Sleep(delay)
		reproduceMessage(t, kBytes, vBytes)
	}(topic, partition, key, value, backoff)
}

func reproduceMessage(topic string, key, value []byte) {
	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Printf("Failed to dial broker for retry: %v", err)
		return
	}
	defer conn.Close()

	authReq := &protocol.Request{
		Type:     protocol.ReqAuthenticate,
		Username: "admin",
		Password: "admin",
	}
	protocol.WriteRequest(conn, authReq)
	protocol.ReadResponse(conn, false)

	req := &protocol.Request{
		Type:  protocol.ReqProduce,
		Topic: topic,
		Key:   key,
		Value: value,
	}
	protocol.WriteRequest(conn, req)
}
