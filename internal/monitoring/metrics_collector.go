package monitoring

import (
	"sync"
	"time"

	"kafkalite/pkg/metrics"
)

type MetricsSnapshot struct {
	MessagesProducedTotal int64   `json:"messages_produced_total"`
	MessagesConsumedTotal int64   `json:"messages_consumed_total"`
	ProduceRate           float64 `json:"produce_rate"`
	ConsumeRate           float64 `json:"consume_rate"`
}

type MetricsCollector struct {
	mu           sync.RWMutex
	snapshot     MetricsSnapshot
	lastProduced int64
	lastConsumed int64
}

var GlobalCollector = &MetricsCollector{}

func init() {
	go GlobalCollector.start()
}

func (m *MetricsCollector) start() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		stats := metrics.GetStats()
		
		totalProd := stats.MessagesProduced
		totalCons := stats.MessagesConsumed

		m.mu.Lock()
		prodRate := float64(totalProd - m.lastProduced)
		consRate := float64(totalCons - m.lastConsumed)

		m.snapshot = MetricsSnapshot{
			MessagesProducedTotal: totalProd,
			MessagesConsumedTotal: totalCons,
			ProduceRate:           prodRate,
			ConsumeRate:           consRate,
		}

		m.lastProduced = totalProd
		m.lastConsumed = totalCons
		m.mu.Unlock()
	}
}

func (m *MetricsCollector) GetSnapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.snapshot
}
