package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"

	"kafkalite/internal/broker"
	"kafkalite/internal/monitoring"
)

func loadConfig() {
	viper.SetConfigName("server")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("./configs")
	viper.SetDefault("broker.id", 0)
	viper.SetDefault("listen.address", "0.0.0.0:9092")
	viper.SetDefault("data.dir", "./data")
	viper.SetDefault("retention.ms", int64(604800000))
	viper.SetDefault("retention.bytes", int64(1073741824))

	if err := viper.ReadInConfig(); err != nil {
		log.Printf("No config file found, using defaults: %v", err)
	}
}

func initLogger(format string) {
	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, nil)
	} else {
		handler = slog.NewTextHandler(os.Stdout, nil)
	}
	slog.SetDefault(slog.New(handler))
}

func startMetricsServer() {
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Println("Starting metrics server on :8080...")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()
}

func toJSON(v []string) string {
	if len(v) == 0 {
		return "[]"
	}
	bytes, _ := json.Marshal(v)
	return string(bytes)
}

func checkWritable(dataDir string) bool {
	testFile := filepath.Join(dataDir, ".healthcheck")
	if err := os.WriteFile(testFile, []byte("ok"), 0666); err != nil {
		return false
	}
	os.Remove(testFile)
	return true
}

func startAdminServer(srv *broker.Server, shutdown chan struct{}) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-shutdown:
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		default:
		}
		if !checkWritable(viper.GetString("data.dir")) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		led := srv.GetLedPartitions()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"alive","is_leader_for":%s,"partition_count":%d}`,
			toJSON(led), len(led))
	})
	go http.ListenAndServe(":8081", mux)
}

func setupShutdown(srv *broker.Server, retentionShutdown chan struct{}) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		slog.Info("Shutting down broker...")
		close(retentionShutdown)
		if err := srv.Close(); err != nil {
			slog.Error("Error closing server", "err", err)
		}
		os.Exit(0)
	}()
}

func main() {
	leader := flag.String("leader", "", "Leader address to sync from")
	brokerID := flag.Int("id", 0, "Broker ID")
	logFormat := flag.String("log-format", "text", "Log format: text or json")
	acks := flag.Int("acks", -1, "Acks: 0 (fire-and-forget), 1 (leader only), -1 (all replicas)")
	flushInterval := flag.Duration("flush-interval", 100*time.Millisecond, "Group commit flush interval")
	batchSize := flag.Int("batch-size", 65536, "Group commit batch size in bytes")
	compactionInterval := flag.Duration("compaction-interval", 5*time.Minute, "Interval for running background compaction")
	rebalanceInterval := flag.Duration("rebalance-interval", 30*time.Second, "Interval for partition rebalancing")
	rebalanceThreshold := flag.Int64("rebalance-threshold", 10000, "Threshold of messages before rebalancing")
	flag.Parse()

	initLogger(*logFormat)
	loadConfig()
	startMetricsServer()

	dataDir := viper.GetString("data.dir")
	listenAddr := viper.GetString("listen.address")

	go monitoring.StartWebSocketServer()

	srv, err := broker.NewServer(*brokerID, dataDir, *acks, *flushInterval, *batchSize, *rebalanceInterval, *rebalanceThreshold)
	if err != nil {
		slog.Error("Failed to initialize server", "err", err)
		os.Exit(1)
	}

	retentionShutdown := make(chan struct{})
	setupShutdown(srv, retentionShutdown)
	startAdminServer(srv, retentionShutdown)

	rm := broker.NewRetentionManager(srv, viper.GetInt64("retention.ms"), viper.GetInt64("retention.bytes"), *compactionInterval)
	rm.Start(retentionShutdown)

	if *leader != "" {
		nodeIDStr := strconv.Itoa(*brokerID)
		go srv.RunFollowerLoop(*leader, nodeIDStr)
	}

	slog.Info("Starting KafkaLite broker...", "addr", listenAddr)
	if err := srv.Start(listenAddr); err != nil {
		slog.Error("Server error", "err", err)
		os.Exit(1)
	}
}
