# KafkaLite: Distributed Commit Log Technical Deep-Dive

This document details the architectural design, low-level data structures, replication safety guarantees, and fault-tolerance loops of **KafkaLite**.

---

## 1. The Wire Protocol (Custom Binary TCP Framing)
Instead of using heavy HTTP or gRPC schemas, KafkaLite implements a custom, length-prefixed binary wire frame protocol defined in [`internal/protocol/protocol.go`](file:///c:/Users/yashn/KafkaLite/internal/protocol/protocol.go).

*   **Request Frame Layout**:
    ```
    ┌─────────────────────────┬──────────────┬─────────────────────────────┐
    │  Frame Length (4 Bytes) │ Type (1 Byte)│      Payload (Variable)     │
    └─────────────────────────┴──────────────┴─────────────────────────────┘
    ```
    *   **Frame Length**: A 32-bit big-endian integer specifying the exact size of the payload following it.
    *   **Type**: A single byte indicating the API request:
        *   `1` = **Produce Request**: Write a record to the broker.
        *   `2` = **Consume Request**: Read a batch of records from a partition.
        *   `3` = **Join Group Request**: A consumer registers with a consumer group.
        *   `4` = **Sync Group Request**: A consumer requests its partition assignments.
        *   `5` = **Offset Commit**: A consumer updates its progress.
        *   `6` = **Offset Fetch**: A consumer retrieves its last committed progress offset.
        *   `7` = **Replicate Request**: Internal leader-to-follower log replication.
*   **Zero-Copy Design**: Payload strings are serialized as `[uint16 length][bytes data]` and raw binaries as `[uint32 length][bytes data]`. The broker parses incoming socket streams directly into byte buffers without dynamic reflection, keeping CPU cycles minimal.

---

## 2. The Storage Engine (Durable Append-Only Logs & Sparse Indexes)
Persistence is managed inside the `internal/storage/` package. Each partition (e.g. topic `test`, partition `0`) has its own subdirectory containing `.log` data files and `.index` offset files.

*   **Log Segment File ([segment.go](file:///c:/Users/yashn/KafkaLite/internal/storage/segment.go))**:
    *   Messages are written sequentially as binary packages:
        `[key_length (4 bytes)][key][value_length (4 bytes)][value]`.
    *   Every append executes a physical disk synchronization flush (`fsync`) to guarantee durability under hardware power cuts.
*   **Sparse Indexing File ([index.go](file:///c:/Users/yashn/KafkaLite/internal/storage/index.go))**:
    *   Indexing every record is highly memory-inefficient. Instead, KafkaLite writes a 16-byte index checkpoint entry (`[logical_offset (8 bytes)][physical_position (8 bytes)]`) to the `.index` file every **4KB** of data written.
    *   To seek a record at logical offset `1024`, the broker performs a binary search on the in-memory sparse index to find the nearest checkpoint (e.g. offset `1000` at physical address `81920`), jumps to that address, and sequentially reads forward until offset `1024` is matched.
*   **Log Recovery**:
    *   Upon starting, `recoverNextOffset()` loads the index checkpoints. If the broker crashed mid-write, it scans the tail of the log file, removes incomplete binary fragments, and truncates the file back to the last valid write boundaries.

---

## 3. Log Retention & Compaction (The Janitor)
To prevent infinite disk utilization and database volume fatigue, KafkaLite implements background log maintenance cleaners inside [`internal/broker/retention.go`](file:///c:/Users/yashn/KafkaLite/internal/broker/retention.go).

*   **Time & Size-Based Retention**:
    *   A background `RetentionManager` runs every 5 minutes enforcing constraints set by `retention.ms` and `retention.bytes`.
    *   If a segment's last modification time exceeds the age threshold (default 7 days) or the total partition directory size exceeds the storage limit (default 1GB), the oldest closed segments are deleted.
*   **Key-Based Log Compaction**:
    *   For key-value states, KafkaLite enforces log compaction. The background janitor parses closed segments sequentially, matching unique keys, and retains only the latest record value per key.
    *   It writes the consolidated entries to a temporary segment and atomically replaces the original segment file, preserving storage space for event-sourcing and log compaction use cases.

---

## 4. High-Availability Replication (Sync Replica Pipeline)
To ensure high availability under host crashes, partition logs are replicated from the partition leader to active followers.

*   **Replication Manager ([replication.go](file:///c:/Users/yashn/KafkaLite/internal/broker/replication.go))**:
    *   When the leader receives a `ReqProduce` request, it appends the message locally and forwards a replication packet (`ReqReplicate`) over TCP to the active followers.
    *   Under the PACELC theorem, we balance consistency and performance using synchronous replication: the leader blocks the producer response until at least one follower responds with a replication confirmation ACK, guaranteeing that the write exists on multiple physical host drives before success is reported.

---

## 5. Client-Side Fault Tolerance (The Retry Loop)
Distributed networks are fundamentally unreliable. The KafkaLite client is designed to recover from node failures automatically.

*   **Metadata Refresh & Retry ([cmd/client/main.go](file:///c:/Users/yashn/KafkaLite/cmd/client/main.go))**:
    *   If the producer writes to a broker that is no longer the partition leader, the broker returns a `Not Leader` error status code.
    *   Upon receiving this status, the client catches the error, pauses for a brief backoff period to allow election convergence, and dials the other seed brokers to request the latest partition routing catalog.
    *   It then updates its local routing cache and retries the publish request against the newly elected partition leader, ensuring zero message drops during failover.

---

## 6. Cluster Coordination & Active Failovers (The Controller)
KafkaLite implements self-healing partition re-allocations without external coordinators like Zookeeper or Raft.

*   **Health Polling ([controller.go](file:///c:/Users/yashn/KafkaLite/internal/broker/controller.go))**:
    *   Each broker runs a background loop polling the other brokers' TCP ports every 5 seconds.
    *   Broker mappings (which node is leader for which partition) are stored in `/data/metadata/leaders.json` on a shared cluster volume.
*   **Atomic Lock Elections**:
    *   If the leader of a partition fails (unreachable TCP port), the surviving brokers immediately attempt to create a lock file (`leaders.json.lock`) atomically using Windows/Unix filesystem exclusion flags (`os.O_CREATE | os.O_EXCL | os.O_WRONLY`).
    *   The broker that successfully acquires the lock reads `leaders.json`, claims leadership of the orphaned partition, flushes the updated configurations, deletes the lock file, and begins serving the partition client traffic.

---

## 7. Consumer Group Scaling (Rebalancing & Offset Commit)
To scale consumption, multiple consumers share the workload using consumer groups.

*   **Rebalance Coordinator ([group_coordinator.go](file:///c:/Users/yashn/KafkaLite/internal/consumer/group_coordinator.go))**:
    *   The broker acts as the coordinator. When consumers dial the coordinator, they register using `ReqJoinGroup`.
    *   The coordinator triggers a rebalance generation and runs a standard **Range Assignor** algorithm: it divides partitions (e.g. partition 0 and partition 1) alphabetically across the registered consumer IDs, ensuring no duplicate processing.
*   **Offset Manager ([offset_manager.go](file:///c:/Users/yashn/KafkaLite/internal/consumer/offset_manager.go))**:
    *   Consumers periodically commit their read progress (`ReqOffsetCommit`).
    *   The offsets are saved as JSON files in the broker's data directory and flushed via `fsync()`, allowing consumers to crash and resume progress safely.

---

## 8. Observability, Logging, and CI/CD

*   **Prometheus Metrics ([stats.go](file:///c:/Users/yashn/KafkaLite/pkg/metrics/stats.go))**: Exposes metric counters on port `:8080/metrics` tracking bytes read/written, message rates, and p99 write latency profiles.
*   **Structured slog**: System logs use Go's structured `slog` library. Administrators can launch the broker with `--log-format json` to pipe logs into parsing search index daemons (like Elasticsearch/Splunk) or keep it in `--log-format text` for standard console reading.
*   **Kubernetes Liveness & Readiness Probes**:
    *   The broker exposes an admin server on a separate port (`8081`).
    *   It returns `200 OK` under the `/health` endpoint only if the data directory is writable and the internal goroutines are active.
    *   This allows orchestration engines (like Kubernetes) to perform liveness/readiness probes, isolating brokers with full disks or startup locks from active traffic routing.
*   **CI/CD Pipeline**: GitHub Actions workflows compile cross-platform binaries, verify test code coverage with the race detector (`make test`), run code analysis checks (`make lint`), and automate container building.
