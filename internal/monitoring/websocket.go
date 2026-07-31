package monitoring

import (
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"kafkalite/internal/auth"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // allow all for demo
	},
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if _, err := auth.ValidateJWT(token); err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Failed to upgrade websocket: %v", err)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			snap := GlobalCollector.GetSnapshot()
			if err := conn.WriteJSON(snap); err != nil {
				return
			}
		}
	}
}

func StartWebSocketServer() {
	http.HandleFunc("/ws/metrics", handleWebSocket)
	log.Println("WebSocket Metrics Server listening on :8083")
	if err := http.ListenAndServe(":8083", nil); err != nil {
		log.Fatalf("WebSocket server failed: %v", err)
	}
}
