package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kafkalite/internal/protocol"
)

func main() {
	source := flag.String("source", "", "DLQ topic to read from")
	target := flag.String("target", "", "Original topic to replay to")
	broker := flag.String("broker", "localhost:9092", "Broker address")
	flag.Parse()

	if *source == "" || *target == "" {
		fmt.Println("Usage: dlq-replay --source <dlq-topic> --target <target-topic>")
		os.Exit(1)
	}

	conn, err := net.Dial("tcp", *broker)
	if err != nil {
		log.Fatalf("Failed to connect to broker: %v", err)
	}
	defer conn.Close()

	authReq := &protocol.Request{
		Type:     protocol.ReqAuthenticate,
		Username: "admin",
		Password: "admin",
	}
	protocol.WriteRequest(conn, authReq)
	protocol.ReadResponse(conn, false)

	offset := int64(0)
	fmt.Printf("Replaying messages from %s to %s...\n", *source, *target)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	for {
		select {
		case <-stop:
			fmt.Println("Shutting down dlq-replay...")
			return
		default:
		}

		req := &protocol.Request{
			Type:     protocol.ReqConsume,
			Topic:    *source,
			Offset:   offset,
			MaxBytes: 1024 * 1024,
		}
		protocol.WriteRequest(conn, req)
		resp, err := protocol.ReadResponse(conn, true)
		if err != nil {
			log.Printf("Consume error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if resp.Status != protocol.StatusOk {
			log.Printf("Broker error: %s", resp.ErrMsg)
			time.Sleep(1 * time.Second)
			continue
		}

		if len(resp.Records) == 0 {
			time.Sleep(1 * time.Second)
			continue
		}

		for _, rec := range resp.Records {
			pReq := &protocol.Request{
				Type:  protocol.ReqProduce,
				Topic: *target,
				Key:   rec.Key,
				Value: rec.Value,
			}
			protocol.WriteRequest(conn, pReq)
			pResp, _ := protocol.ReadResponse(conn, false)
			if pResp.Status == protocol.StatusOk {
				fmt.Printf("Replayed message offset %d from DLQ to target offset %d\n", rec.Offset, pResp.Offset)
			}
			offset = rec.Offset + 1
		}
	}
}
