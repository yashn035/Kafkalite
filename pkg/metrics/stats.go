package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	MessagesProduced = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_produced_total",
			Help: "Total number of messages produced to a topic.",
		},
		[]string{"topic"},
	)

	MessagesConsumed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_consumed_total",
			Help: "Total number of messages consumed from a topic.",
		},
		[]string{"topic"},
	)

	ProduceLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kafka_produce_latency_seconds",
			Help:    "Latency in seconds for produce requests.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.1},
		},
		[]string{"topic"},
	)

	PartitionSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_partition_size_bytes",
			Help: "Current size in bytes of the partition log segment.",
		},
		[]string{"topic"},
	)
)

func init() {
	prometheus.MustRegister(MessagesProduced)
	prometheus.MustRegister(MessagesConsumed)
	prometheus.MustRegister(ProduceLatency)
	prometheus.MustRegister(PartitionSize)
}

func RecordProduce(topic string, size int64, duration time.Duration) {
	MessagesProduced.WithLabelValues(topic).Inc()
	ProduceLatency.WithLabelValues(topic).Observe(duration.Seconds())
	PartitionSize.WithLabelValues(topic).Set(float64(size))
}

func RecordConsume(topic string, count int) {
	MessagesConsumed.WithLabelValues(topic).Add(float64(count))
}
type DummyStruct struct{} // Avoid golint empty file warnings
