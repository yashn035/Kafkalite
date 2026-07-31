package broker

import (
	"log/slog"
	"time"

	"kafkalite/pkg/metrics"
)

type Rebalancer struct {
	controller *Controller
	interval   time.Duration
	threshold  int64
	shutdown   chan struct{}
}

func NewRebalancer(controller *Controller, interval time.Duration, threshold int64) *Rebalancer {
	return &Rebalancer{
		controller: controller,
		interval:   interval,
		threshold:  threshold,
		shutdown:   make(chan struct{}),
	}
}

func (r *Rebalancer) Start() {
	go func() {
		ticker := time.NewTicker(r.interval)
		defer ticker.Stop()
		for {
			select {
			case <-r.shutdown:
				return
			case <-ticker.C:
				r.checkLoad()
			}
		}
	}()
}

func (r *Rebalancer) Stop() {
	close(r.shutdown)
}

func (r *Rebalancer) checkLoad() {
	stats := metrics.GetStats()
	if stats.MessagesProduced > r.threshold {
		slog.Warn("High load detected. Auto rebalancing partitions...")
		// Simple logic: If load is very high, push a partition to another broker.
		// For example, if broker 0 is leading test-0, move it to broker 1.
		r.controller.MovePartition("test-0", (r.controller.brokerID+1)%3)
	}
}
