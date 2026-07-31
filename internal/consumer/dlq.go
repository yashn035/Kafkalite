package consumer

import (
	"fmt"
	"log"
	"net"

	"kafkalite/internal/protocol"
)

func SendToDLQ(originalTopic string, partition int32, key, value []byte, reason string) {
	dlqTopic := fmt.Sprintf("%s-dlq", originalTopic)
	
	// We can prefix the value with reason or send headers if we had them.
	// We will just send it to the DLQ topic.
	log.Printf("Sending message to DLQ: %s, Reason: %s", dlqTopic, reason)

	conn, err := net.Dial("tcp", "localhost:9092")
	if err != nil {
		log.Printf("Failed to dial broker for DLQ: %v", err)
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
		Topic: dlqTopic,
		Key:   key,
		Value: value,
	}
	protocol.WriteRequest(conn, req)
}
