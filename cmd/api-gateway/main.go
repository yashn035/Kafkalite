package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"kafkalite/internal/ai"
	"kafkalite/internal/auth"
	"kafkalite/internal/protocol"
)

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" ||
		   r.URL.Path == "/favicon.ico" ||
		   r.URL.Path == "/health" ||
		   r.URL.Path == "/auth" ||
		   strings.HasPrefix(r.URL.Path, "/static/") ||
		   strings.HasSuffix(r.URL.Path, ".css") ||
		   strings.HasSuffix(r.URL.Path, ".js") ||
		   strings.HasSuffix(r.URL.Path, ".png") ||
		   strings.HasSuffix(r.URL.Path, ".ico") {
			next.ServeHTTP(w, r)
			return
		}
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		token := strings.TrimPrefix(authHeader, "Bearer ")
		claims, err := auth.ValidateJWT(token)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		// In a real app we'd pass claims via context.
		_ = claims
		next.ServeHTTP(w, r)
	})
}

func readAuthResponse(r io.Reader) error {
	var totalLen int32
	if err := binary.Read(r, binary.BigEndian, &totalLen); err != nil {
		return err
	}
	statusBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, statusBuf); err != nil {
		return err
	}
	if statusBuf[0] == protocol.StatusErr {
		var errLen uint16
		binary.Read(r, binary.BigEndian, &errLen)
		errMsgBuf := make([]byte, errLen)
		io.ReadFull(r, errMsgBuf)
		return fmt.Errorf("auth error: %s", string(errMsgBuf))
	}
	var tokenLen uint16
	if err := binary.Read(r, binary.BigEndian, &tokenLen); err != nil {
		return err
	}
	tokenBuf := make([]byte, tokenLen)
	if _, err := io.ReadFull(r, tokenBuf); err != nil {
		return err
	}
	return nil
}

func main() {
	addr := flag.String("addr", ":8082", "API Gateway bind address")
	brokerAddr := flag.String("broker", "broker-0:9092", "Broker address to connect to")
	flag.Parse()

	if portEnv := os.Getenv("PORT"); portEnv != "" && *addr == ":8082" {
		*addr = ":" + portEnv
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/topics", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`["test-0", "test-1"]`))
		case "POST":
			w.WriteHeader(http.StatusCreated)
		}
	})

	mux.HandleFunc("/produce", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Topic      string `json:"topic"`
			Key        string `json:"key"`
			Value      string `json:"value"`
			MessageID  string `json:"message_id"`
			ProducerID int64  `json:"producer_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		conn, err := net.Dial("tcp", *brokerAddr)
		if err != nil {
			http.Error(w, "Broker unavailable", http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		// Auth with broker
		authReq := &protocol.Request{
			Type:     protocol.ReqAuthenticate,
			Username: "admin",
			Password: "admin",
		}
		protocol.WriteRequest(conn, authReq)
		readAuthResponse(conn)

		req := &protocol.Request{
			Type:       protocol.ReqProduce,
			Topic:      body.Topic,
			Key:        []byte(body.Key),
			Value:      []byte(body.Value),
			MessageID:  body.MessageID,
			ProducerID: body.ProducerID,
		}
		protocol.WriteRequest(conn, req)
		resp, _ := protocol.ReadResponse(conn, false)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"offset": resp.Offset, "status": resp.Status})
	})

	mux.HandleFunc("/consume", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		max := 100
		if maxStr := r.URL.Query().Get("max"); maxStr != "" {
			if parsed, err := strconv.Atoi(maxStr); err == nil && parsed > 0 {
				max = parsed
			}
		}

		topic := r.URL.Query().Get("topic")
		dialer := net.Dialer{}
		conn, err := dialer.DialContext(ctx, "tcp", *brokerAddr)
		if err != nil {
			http.Error(w, "Broker unavailable", http.StatusServiceUnavailable)
			return
		}
		defer conn.Close()

		authReq := &protocol.Request{
			Type:     protocol.ReqAuthenticate,
			Username: "admin",
			Password: "admin",
		}
		protocol.WriteRequest(conn, authReq)
		readAuthResponse(conn)

		var startTime, endTime int64
		if st := r.URL.Query().Get("start_time"); st != "" {
			if parsed, err := strconv.ParseInt(st, 10, 64); err == nil {
				startTime = parsed
			}
		}
		if et := r.URL.Query().Get("end_time"); et != "" {
			if parsed, err := strconv.ParseInt(et, 10, 64); err == nil {
				endTime = parsed
			}
		}

		req := &protocol.Request{
			Type:      protocol.ReqConsume,
			Topic:     topic,
			Offset:    0, // Simplify for demo
			MaxBytes:  1024 * 1024,
			StartTime: startTime,
			EndTime:   endTime,
		}
		protocol.WriteRequest(conn, req)
		resp, err := protocol.ReadResponse(conn, true)
		if err != nil {
			http.Error(w, "Consume failed", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if len(resp.Records) == 0 {
			w.Write([]byte(`{"messages": []}`))
			return
		}
		if len(resp.Records) > max {
			resp.Records = resp.Records[:max]
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"messages": resp.Records})
	})

	mux.HandleFunc("/schemas", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Topic  string `json:"topic"`
			Schema string `json:"schema"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		conn, err := net.Dial("tcp", *brokerAddr)
		if err != nil {
			http.Error(w, "Broker unavailable", http.StatusInternalServerError)
			return
		}
		defer conn.Close()

		authReq := &protocol.Request{
			Type:     protocol.ReqAuthenticate,
			Username: "admin",
			Password: "admin",
		}
		protocol.WriteRequest(conn, authReq)
		readAuthResponse(conn)

		req := &protocol.Request{
			Type:      protocol.ReqRegisterSchema,
			Topic:     body.Topic,
			SchemaDef: body.Schema,
		}
		protocol.WriteRequest(conn, req)
		resp, _ := protocol.ReadResponse(conn, false)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"status": resp.Status})
	})

	mux.HandleFunc("/ai/insights", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		
		insights := ai.AnalyzeCluster()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(insights)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})

	mux.HandleFunc("/auth", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		if body.Username == "admin" && body.Password == "admin123" {
			token, err := auth.GenerateJWT("admin", "admin")
			if err != nil {
				http.Error(w, "Error generating token", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]string{"token": token})
			return
		}
		
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	})

	// Serve static frontend files
	frontendDir := "web/frontend"
	if _, err := os.Stat(frontendDir); err == nil {
		fs := http.FileServer(http.Dir(frontendDir))
		mux.Handle("/", fs)
	} else {
		// Fallback: return a simple HTML page
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/" {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprintf(w, `<html><body><h1>KafkaLite API Gateway</h1><p>Frontend not found. Please check the deployment.</p></body></html>`)
		})
	}

	fmt.Printf("API Gateway listening on %s\n", *addr)
	log.Fatal(http.ListenAndServe(*addr, corsMiddleware(authMiddleware(mux))))
}
