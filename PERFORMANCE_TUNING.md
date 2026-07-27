# KafkaLite: Performance Tuning Guide

This document describes how to optimize the storage throughput and latency profiles of your KafkaLite broker cluster.

---

## 🛠️ 1. Configurations Tuning (`configs/server.yaml`)

Your default broker configuration is managed in `./configs/server.yaml`. You can modify these configurations to balance disk utilization, memory usage, and durability:

```yaml
broker:
  id: 0
listen:
  address: "0.0.0.0:9092"
data:
  dir: "./data"
retention:
  ms: 604800000        # Segment retention age threshold (default 7 days in ms)
  bytes: 1073741824    # Maximum disk usage per partition (default 1GB in bytes)
```

---

## 💾 2. Durability vs. Latency: The `fsync` Trade-off

KafkaLite calls `fsync()` on every single write operation inside [segment.go](file:///c:/Users/yashn/KafkaLite/internal/storage/segment.go) to guarantee that bytes are physically flushed to persistent media. This is the **strongest durability level** (preventing data loss during OS crashes/power failures), but it is limited by disk physics:
* **Fsync on Every Write**: Limits single-client message rates to the drive's physical write speeds.
* **To Optimize (Group Committing)**: In high-throughput settings, client records can be batched together. Instead of sending 1,000 requests, send a single request containing a slice of 1,000 records. This amortizes the cost of a single `fsync` flush across all records, boosting write speed past **100k msg/sec**.

---

## 📅 3. Adjusting Log Retention & Compaction

Background cleanups run inside [retention.go](file:///c:/Users/yashn/KafkaLite/internal/broker/retention.go). 
* **Size-based Purging**: Set `retention.bytes` lower (e.g. `104857600` for 100MB) to prune old closed segments as soon as disk utilization is exceeded.
* **Age-based Purging**: Set `retention.ms` lower (e.g., `3600000` for 1 hour) to auto-prune stale log segments.
* **Key-based Compaction**: Compaction runs in the background, matching unique keys and keeping only the latest value. To prevent performance bottlenecks on active writes, schedule compaction routines during off-peak times or reduce segment file sizes (e.g., to 100MB) to run merges on closed blocks.

---

## 🔍 4. Tweaking Sparse Index Checkpoints

In [segment.go](file:///c:/Users/yashn/KafkaLite/internal/storage/segment.go), KafkaLite writes index checkpoints to disk:

```go
if physicalPos == 0 || s.size-s.lastIndexedSize >= 4096 {
    s.index.AddEntry(logicalOffset, physicalPos)
}
```

* **The 4KB Default**: Matches standard SSD page sizes.
* **Tuning Down (e.g., 1KB)**: Write more checkpoints. Increases index file size on disk and in-memory footprint, but speeds up seeks on large partitions by minimizing the final sequential file scan size.
* **Tuning Up (e.g., 64KB)**: Minimizes index size and disk write operations, but raises read latencies on large partitions as the reader must scan up to 64KB of raw binary files sequentially.
