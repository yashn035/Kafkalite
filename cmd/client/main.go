package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"time"

	"kafkalite/internal/protocol"
)

var (
	mode       = flag.String("mode", "", "Mode: produce or consume")
	broker     = flag.String("broker", "localhost:9092", "Broker address")
	addrFlag   = flag.String("addr", "", "Alias for broker address")
	topic      = flag.String("topic", "", "Topic name")
	messages   = flag.Int("messages", 1, "Number of messages to produce")
	group      = flag.String("group", "", "Consumer group ID")
	partition  = flag.Int("partition", -1, "Partition (default -1 means auto-balance)")
	consumerID = flag.String("consumer-id", "", "Unique consumer ID")
	logEnabled = flag.Bool("log", false, "Enable verbose logging")
	benchmark  = flag.String("benchmark", "", "Benchmark mode: produce or consume")
)

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  client --mode produce --topic <topic> --messages <n>")
	fmt.Println("  client --mode consume --topic <topic> --group <group>")
}

func runProduceClient(bAddr, t string, args []string) {
	conn, err := net.Dial("tcp", bAddr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	part := *partition
	if part == -1 {
		part = 0
	}
	targetTopic := fmt.Sprintf("%s-%d", t, part)

	if *messages > 1 || len(args) < 2 {
		for i := 0; i < *messages; i++ {
			k := fmt.Sprintf("key-%d", i)
			v := fmt.Sprintf("val-%d", i)
			sendProduce(conn, targetTopic, k, v)
		}
	} else {
		sendProduce(conn, targetTopic, args[0], args[1])
	}
}

func sendProduce(conn net.Conn, t, k, v string) {
	req := &protocol.Request{
		Type:  protocol.ReqProduce,
		Topic: t,
		Key:   []byte(k),
		Value: []byte(v),
	}
	if err := protocol.WriteRequest(conn, req); err != nil {
		log.Fatalf("Failed to write request: %v", err)
	}
	resp, err := protocol.ReadResponse(conn, false)
	if err != nil {
		log.Fatalf("Failed to read response: %v", err)
	}
	if resp.Status == protocol.StatusErr {
		log.Fatalf("Broker error: %s", resp.ErrMsg)
	}
	if *logEnabled {
		fmt.Printf("[%s] Produced key=%s to topic=%s at offset=%d\n", *consumerID, k, t, resp.Offset)
	}
}

func runConsumeClient(bAddr, t string) {
	mID := *consumerID
	if mID == "" {
		hostname, _ := os.Hostname()
		mID = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}

	conn, err := net.Dial("tcp", bAddr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	if *group != "" {
		_, parts := joinGroup(conn, *group, mID, t)
		if len(parts) == 0 {
			fmt.Println("No partitions assigned.")
			return
		}
		for _, p := range parts {
			go consumePartitionLoop(t, p)
		}
		select {}
	} else {
		p := int32(*partition)
		if p == -1 {
			p = 0
		}
		consumePartitionLoop(t, p)
	}
}

func joinGroup(conn net.Conn, gID, mID, t string) (int, []int32) {
	req := &protocol.Request{
		Type:     protocol.ReqJoinGroup,
		GroupID:  gID,
		MemberID: mID,
		Topics:   []string{t},
	}
	if err := protocol.WriteRequest(conn, req); err != nil {
		log.Fatalf("JoinGroup send failed: %v", err)
	}
	resp, err := protocol.ReadGroupResponse(conn)
	if err != nil {
		log.Fatalf("JoinGroup read failed: %v", err)
	}
	if resp.Status == protocol.StatusErr {
		log.Fatalf("JoinGroup error: %s", resp.ErrMsg)
	}

	syncReq := &protocol.Request{
		Type:     protocol.ReqSyncGroup,
		GroupID:  gID,
		MemberID: mID,
	}
	if err := protocol.WriteRequest(conn, syncReq); err != nil {
		log.Fatalf("SyncGroup send failed: %v", err)
	}
	syncResp, err := protocol.ReadGroupResponse(conn)
	if err != nil {
		log.Fatalf("SyncGroup read failed: %v", err)
	}
	return resp.Generation, syncResp.Assignment[t]
}

func consumePartitionLoop(t string, p int32) {
	partTopic := fmt.Sprintf("%s-%d", t, p)
	offset := fetchStartOffset(t, p)

	for {
		conn, err := dialPartitionLeader(partTopic)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		nextOff, err := executePartitionConsume(conn, partTopic, offset, p)
		conn.Close()

		if err == nil {
			offset = nextOff
		}
		time.Sleep(1 * time.Second)
	}
}

func fetchStartOffset(t string, p int32) int64 {
	if *group == "" {
		return 0
	}
	conn, err := net.Dial("tcp", *broker)
	if err != nil {
		return 0
	}
	defer conn.Close()

	req := &protocol.Request{
		Type:      protocol.ReqOffsetFetch,
		GroupID:   *group,
		Topic:     t,
		Partition: p,
	}
	if err := protocol.WriteRequest(conn, req); err != nil {
		return 0
	}
	resp, err := protocol.ReadResponse(conn, false)
	if err != nil || resp.Status == protocol.StatusErr || resp.Offset == -1 {
		return 0
	}
	return resp.Offset
}

func executePartitionConsume(conn net.Conn, partTopic string, offset int64, p int32) (int64, error) {
	req := &protocol.Request{
		Type:     protocol.ReqConsume,
		Topic:    partTopic,
		Offset:   offset,
		MaxBytes: 1048576,
	}
	if err := protocol.WriteRequest(conn, req); err != nil {
		return offset, err
	}
	resp, err := protocol.ReadResponse(conn, true)
	if err != nil {
		return offset, err
	}
	if resp.Status == protocol.StatusErr {
		return offset, fmt.Errorf("broker error: %s", resp.ErrMsg)
	}
	if len(resp.Records) > 0 {
		for _, rec := range resp.Records {
			fmt.Printf("[%s] Partition %d: %s => %s\n", *consumerID, p, string(rec.Key), string(rec.Value))
		}
		if *group != "" {
			commitOffset(*group, partTopic, p, resp.Offset)
		}
	}
	return resp.Offset, nil
}

func commitOffset(gID, partTopic string, p int32, offset int64) {
	conn, err := net.Dial("tcp", *broker)
	if err != nil {
		return
	}
	defer conn.Close()

	req := &protocol.Request{
		Type:      protocol.ReqOffsetCommit,
		GroupID:   gID,
		Topic:     partTopic,
		Partition: p,
		Offset:    offset,
	}
	protocol.WriteRequest(conn, req)
	protocol.ReadResponse(conn, false)
}

func dialPartitionLeader(partTopic string) (net.Conn, error) {
	brokers := []string{"localhost:9092", "localhost:9093", "localhost:9094"}
	for _, addr := range brokers {
		conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
		if err != nil {
			continue
		}
		req := &protocol.Request{
			Type:     protocol.ReqConsume,
			Topic:    partTopic,
			Offset:   0,
			MaxBytes: 1,
		}
		protocol.WriteRequest(conn, req)
		resp, err := protocol.ReadResponse(conn, true)
		if err == nil && resp.Status != protocol.StatusErr {
			return conn, nil
		}
		conn.Close()
	}
	return nil, fmt.Errorf("no leader found for partition %s", partTopic)
}

func runProduceBenchmark(bAddr, t string) {
	conn, err := net.Dial("tcp", bAddr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	n := *messages
	latencies := make([]time.Duration, n)
	start := time.Now()

	for i := 0; i < n; i++ {
		reqStart := time.Now()
		req := &protocol.Request{
			Type:  protocol.ReqProduce,
			Topic: t,
			Key:   []byte("benchkey"),
			Value: []byte("benchval"),
		}
		protocol.WriteRequest(conn, req)
		protocol.ReadResponse(conn, false)
		latencies[i] = time.Since(reqStart)
	}

	elapsed := time.Since(start)
	printProduceBenchmarkReport(n, elapsed, latencies)
}

func printProduceBenchmarkReport(n int, elapsed time.Duration, latencies []time.Duration) {
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	tps := float64(n) / elapsed.Seconds()
	p50 := latencies[n*50/100]
	p95 := latencies[n*95/100]
	p99 := latencies[n*99/100]

	fmt.Println("--------------------------------------------------")
	fmt.Println(" KAFKALITE PRODUCE BENCHMARK SUMMARY")
	fmt.Println("--------------------------------------------------")
	fmt.Printf("Total Messages : %d\n", n)
	fmt.Printf("Total Elapsed  : %v\n", elapsed)
	fmt.Printf("Throughput     : %.2f msg/sec\n", tps)
	fmt.Printf("Latency p50    : %v\n", p50)
	fmt.Printf("Latency p95    : %v\n", p95)
	fmt.Printf("Latency p99    : %v\n", p99)
	fmt.Println("--------------------------------------------------")
}

func runConsumeBenchmark(bAddr, t string) {
	conn, err := net.Dial("tcp", bAddr)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	var offset int64 = 0
	totalConsumed := 0
	start := time.Now()
	deadline := start.Add(10 * time.Second)

	for time.Now().Before(deadline) {
		req := &protocol.Request{
			Type:     protocol.ReqConsume,
			Topic:    t + "-0",
			Offset:   offset,
			MaxBytes: 1048576,
		}
		protocol.WriteRequest(conn, req)
		resp, err := protocol.ReadResponse(conn, true)
		if err != nil || resp.Status == protocol.StatusErr {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		totalConsumed += len(resp.Records)
		offset = resp.Offset
		if len(resp.Records) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	printConsumeBenchmarkReport(totalConsumed, time.Since(start))
}

func printConsumeBenchmarkReport(total int, elapsed time.Duration) {
	tps := float64(total) / elapsed.Seconds()
	fmt.Println("--------------------------------------------------")
	fmt.Println(" KAFKALITE CONSUME BENCHMARK SUMMARY")
	fmt.Println("--------------------------------------------------")
	fmt.Printf("Total Messages : %d\n", total)
	fmt.Printf("Total Elapsed  : %v\n", elapsed)
	fmt.Printf("Throughput     : %.2f msg/sec\n", tps)
	fmt.Println("--------------------------------------------------")
}

func main() {
	flag.Parse()

	bAddr := *broker
	if *addrFlag != "" {
		bAddr = *addrFlag
	}

	t := *topic
	m := *mode

	args := flag.Args()
	var payloadArgs []string
	if m == "" && len(args) >= 2 {
		m = args[0]
		t = args[1]
		payloadArgs = args[2:]
	} else {
		payloadArgs = args
	}

	if *benchmark != "" {
		if t == "" {
			t = "test"
		}
		if *benchmark == "produce" {
			runProduceBenchmark(bAddr, t)
		} else {
			runConsumeBenchmark(bAddr, t)
		}
		return
	}

	if m == "" || t == "" {
		printUsage()
		os.Exit(1)
	}

	if m == "produce" {
		runProduceClient(bAddr, t, payloadArgs)
	} else {
		runConsumeClient(bAddr, t)
	}
}
