# Tough Interview Q&A Drill

CONCISE, BULLET-PROOF ANSWERS TO THE 10 HARDEST DISTRIBUTED SYSTEMS QUESTIONS

---

### Q1: Why did you use file-based locks for leader election instead of a proper consensus algorithm like Raft? What happens if the lock file gets corrupted?
> *"We utilized file-based locks inside [controller.go](file:///c:/Users/yashn/KafkaLite/internal/broker/controller.go) because the nodes share a high-availability Docker volume, allowing us to leverage atomic filesystem write exclusion locks (`O_EXCL` flags) for zero-dependency coordination. If the lock file is corrupted or deleted mid-operation, the next health-check poll (which runs every 5 seconds) will detect the missing state, re-acquire the lock atomically, reload `/data/metadata/leaders.json`, and recover partition mappings safely. This design trades Raft's complex peer negotiation code for database-level write guarantees, simplifying operational maintenance."*

---

### Q2: Your replication is synchronous (waiting for 1 ACK). If the follower is slow, does it block the producer? How do you handle this?
> *"Yes, because the leader's `Replicate` call in [replication.go](file:///c:/Users/yashn/KafkaLite/internal/broker/replication.go) blocks until it receives an ACK, a slow follower directly affects the producer's write latency. In a production environment, we would handle this by introducing a write timeout and shifting replication channels to asynchronous workers with bounded ring buffers. If a replica lags beyond a threshold, we drop it from the in-sync replica (ISR) set, allowing the leader to continue accepting writes without blocking the producer."*

---

### Q3: Your sparse index checkpoints every 4KB. Why 4KB? How does this choice affect p99 latency?
> *"The 4KB checkpoint size in [segment.go](file:///c:/Users/yashn/KafkaLite/internal/storage/segment.go) matches the physical page block size of standard SSDs, ensuring index writes align perfectly with hardware boundaries and do not trigger page split writes. A smaller checkpoint size (e.g. 1KB) would increase index size and memory usage, causing cache thrashing and raising p99 latency due to memory overhead. A larger checkpoint size (e.g. 64KB) would decrease index memory footprint but increase p99 search latency by forcing the search to perform long sequential read scans across disk blocks."*

---

### Q4: You use `fsync` on every write. How does this impact your 100k msg/sec claim? How would you optimize this without losing durability?
> *"Calling `fsync()` on every single append in [segment.go](file:///c:/Users/yashn/KafkaLite/internal/storage/segment.go) blocks the write thread until data is physically flushed, limiting single-record throughput to the physical disk rotation speed. To achieve the 100k messages/sec throughput benchmark, we utilize bulk producing (pipelining messages together) or asynchronous flush queues. To optimize this without losing durability, we can implement **Group Committing**, buffering client writes in memory and executing a single `fsync` call for a batch of requests, amortizing disk flush latency across thousands of records."*

---

### Q5: In your consumer group rebalancing, you assign partitions 0 and 1. What happens if a 3rd consumer joins? How does your Range Assignor handle uneven loads?
> *"Our Range Assignor inside [group_coordinator.go](file:///c:/Users/yashn/KafkaLite/internal/consumer/group_coordinator.go) divides partition counts by member count (`limit = p / n`) and distributes any remainders to the first members. If a 3rd consumer joins a topic with only 2 partitions, the assignment division yields a limit of 0 and a remainder of 2, assigning partition 0 to consumer 1 and partition 1 to consumer 2, while consumer 3 remains idle as an active hot standby. In production, we balance uneven loads by scaling the number of partitions to be a multiple of the active consumers count."*

---

### Q6: Your compaction rewrites the entire segment. For a 1GB segment, this is expensive. How would you improve this?
> *"Our compaction logic in [retention.go](file:///c:/Users/yashn/KafkaLite/internal/broker/retention.go) currently rewrites segment logs in a single blocking pass. We can optimize this by breaking large log partitions into smaller, immutable segment files (e.g., 100MB each) and running a background merge loop that compacts closed segments, leaving the active segment untouched. This mimics standard LSM-tree compaction patterns, avoiding large sequential disk writes and preventing system write pauses."*

---

### Q7: How do you handle `SIGKILL` if the broker goes down mid-write? Walk me through your crash recovery process.
> *"Because Go's file appends are sequential, a sudden crash or `SIGKILL` can leave a partially written key-value boundary at the end of the raw `.log` file. During boot, [segment.go](file:///c:/Users/yashn/KafkaLite/internal/storage/segment.go) calls `recoverNextOffset()`, which loads the last valid logical offset entry from the `.index` checkpoint file, reads sequentially from that physical position, discards any trailing corrupted records, truncates the file back to the last valid boundary, and resets the correct write offset."*

---

### Q8: Can your protocol handle backpressure? What happens if the producer sends 10,000 messages faster than the disk can flush?
> *"Our custom TCP wire protocol in [protocol.go](file:///c:/Users/yashn/KafkaLite/internal/protocol/protocol.go) handles backpressure implicitly via TCP's window size controls. Because the leader processes requests synchronously and flushes to disk using `fsync()` before sending a TCP reply, a slow disk flush forces the broker to stop reading from the socket. This fills the OS TCP receive buffers, which triggers the TCP window size update to 0, blocking the producer client's socket writes and naturally matching write speed with disk hardware limit."*

---

### Q9: You have a single HTTP metrics server on port 8080. If that server panics, does the entire broker crash?
> *"Yes, because Go's default HTTP server handler runs on goroutines spawned within the same process space, an unrecovered panic in the metric handler would bubble up and terminate the entire broker binary. We prevent this by recovering panics inside HTTP route handlers or running the server inside a separate Goroutine loop that catches panics and prevents process crashes, isolating metric collection failures from the core TCP broker routing."*

---

### Q10: If I wanted exactly-once semantics (idempotent producer), how would you modify your `segment.go` to handle this?
> *"We would modify the binary protocol request in [protocol.go](file:///c:/Users/yashn/KafkaLite/internal/protocol/protocol.go) and [segment.go](file:///c:/Users/yashn/KafkaLite/internal/storage/segment.go) to include a unique `ProducerID` and a monotonically increasing `SequenceNumber` for each batch. The broker segment would track the latest sequence number written per `ProducerID` in-memory. If a duplicate sequence number is received (indicating a retry), the broker discards the write and returns the cached offset, preventing duplicate records."*
