package ai

import (
	"math/rand"

	"kafkalite/pkg/metrics"
)

type Insight struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

type AIResponse struct {
	Insights   []Insight `json:"insights"`
	Status     string    `json:"status"`
	CPULoad    float64   `json:"cpu_load_percent"`
	TotalStats struct {
		Produced int64 `json:"produced"`
		Consumed int64 `json:"consumed"`
	} `json:"total_stats"`
}

// AnalyzeCluster evaluates broker metrics and returns rule-based AI insights.
func AnalyzeCluster() AIResponse {
	stats := metrics.GetStats()

	// Simulate CPU load (e.g. 10% to 95%)
	cpuLoad := 10.0 + rand.Float64()*85.0

	var insights []Insight

	// Rule 1: High CPU Load
	if cpuLoad > 90.0 {
		insights = append(insights, Insight{
			Level:   "CRITICAL",
			Message: "CPU usage is extremely high (>90%). Consider scaling out brokers or pausing non-essential producers.",
		})
	} else if cpuLoad > 75.0 {
		insights = append(insights, Insight{
			Level:   "WARNING",
			Message: "CPU usage is elevated. Monitor for potential degraded performance.",
		})
	} else {
		insights = append(insights, Insight{
			Level:   "INFO",
			Message: "CPU usage is normal.",
		})
	}

	// Rule 2: Partition Lag (Produced - Consumed)
	lag := stats.MessagesProduced - stats.MessagesConsumed
	if lag > 10000 {
		insights = append(insights, Insight{
			Level:   "WARNING",
			Message: "High consumer lag detected (>10k messages). Add more consumer instances to the group.",
		})
	} else if lag < 0 {
		// Just in case
		lag = 0
	}

	if lag == 0 && stats.MessagesProduced > 0 {
		insights = append(insights, Insight{
			Level:   "INFO",
			Message: "Consumers are fully caught up with producers. Optimal health.",
		})
	}

	return AIResponse{
		Insights: insights,
		Status:   "analyzed",
		CPULoad:  cpuLoad,
		TotalStats: struct {
			Produced int64 `json:"produced"`
			Consumed int64 `json:"consumed"`
		}{
			Produced: stats.MessagesProduced,
			Consumed: stats.MessagesConsumed,
		},
	}
}
