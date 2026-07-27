# High-Level Design (HLD) Whiteboard Script

This script prepares you to present the architectural design of **KafkaLite** in a standard 30-minute system design whiteboard interview.

---

## 🎨 Step 1: The Broker Topology (Draw 3 Boxes)
* **What to Draw**: Draw three large boxes side-by-side. Label them `Broker-0 (9092)`, `Broker-1 (9093)`, and `Broker-2 (9094)`. Connect them with dotted lines indicating a shared network overlay. Below them, draw a cylinder labeled `Shared Metadata Volume (/data/metadata)`.
* **What to Say**: 
  > *"I will start by defining the physical topology of the cluster. We have a 3-node broker topology connected over TCP. Instead of deploying an external consensus ensemble like Zookeeper or Raft, which adds operational complexity and serialization overhead, the brokers mount a high-availability shared network volume. This volume acts as our source of truth for partition leadership mapping."*

---

## 🎨 Step 2: Commit Log & Indexes (Draw Segments inside Box 0)
* **What to Draw**: Inside the `Broker-0` box, draw a horizontal block divided into sections labeled `000.log`, `001.log`. Below it, draw a smaller block labeled `000.index`, `001.index`. Draw an arrow pointing from a logical index entry `Offset 500` to a specific offset block in `000.log` labeled `Pos 8192`.
* **What to Say**:
  > *"Each broker manages partition logs sequentially. Instead of using a B-Tree which introduces write amplification and random page flushes, we write records sequentially in append-only log segments ([segment.go](file:///c:/Users/yashn/KafkaLite/internal/storage/segment.go)), utilizing disk I/O efficiently. To support fast O(log N) random lookups without high memory overhead, we build a sparse index ([index.go](file:///c:/Users/yashn/KafkaLite/internal/storage/index.go)) which writes a 16-byte logical-offset-to-physical-position checkpoint entry every 4KB of log data. Lookups binary-search this in-memory index to locate the nearest physical position, then scan forward sequentially."*

---

## 🎨 Step 3: Replication Pipeline (Draw arrows between Brokers)
* **What to Draw**: Draw a solid line from a `Producer` block pointing to `Broker-0 (Leader)`. Inside `Broker-0`, draw a component labeled `ReplicationManager`. Draw arrows from `ReplicationManager` to `Broker-1` and `Broker-2 (Followers)`. Label the arrow returning from `Broker-1` to `Broker-0` as `Replication ACK`.
* **What to Say**:
  > *"KafkaLite balances write durability and latency using synchronous replication ([replication.go](file:///c:/Users/yashn/KafkaLite/internal/broker/replication.go)). When a Producer publishes to the partition leader, the ReplicationManager pushes the message in parallel to all active followers over TCP. The write is only acknowledged to the producer when at least one follower returns an ACK. This trade-off represents a PACELC guarantee prioritizing write durability over absolute write speed, ensuring zero data loss during a leader crash."*

---

## 🎨 Step 4: Leader Election Controller (Draw Lock on Metadata)
* **What to Draw**: Draw a small padlock icon over the `/data/metadata` database cylinder. Show an arrow from `Broker-1` creating `leaders.json.lock` atomically. Show the file `/data/metadata/leaders.json` mapping `test-0 -> Broker-1`.
* **What to Say**:
  > *"To handle active node failures without Zookeeper, each broker runs a background loop checking leader health every 5 seconds ([controller.go](file:///c:/Users/yashn/KafkaLite/internal/broker/controller.go)). If the leader of a partition dies, the active brokers attempt to create a lock file (`leaders.json.lock`) atomically using cross-platform `O_EXCL` flags. The broker that acquires the lock reads `/data/metadata/leaders.json`, reassigns the partition to itself (since it has the next active broker ID), and writes the mapping. This guarantees a single, coordinated leader election process with zero split-brain risk."*

---

## 🎨 Step 5: Consumer Group Coordination (Draw 2 Consumers)
* **What to Draw**: Draw two boxes at the bottom labeled `Consumer A` and `Consumer B`. Show arrows pointing from `Consumer A` to `Broker-0 (Partition 0)` and `Consumer B` to `Broker-1 (Partition 1)`. Inside the brokers, draw a box labeled `GroupCoordinator` pointing to a rebalance list.
* **What to Say**:
  > *"Finally, we scale consumption using Consumer Groups managed by `GroupCoordinator` ([group_coordinator.go](file:///c:/Users/yashn/KafkaLite/internal/consumer/group_coordinator.go)). When consumers join the group, the coordinator tracks them and executes a **Range Assignor** rebalancing algorithm. It divides partitions 0 and 1 of the topic alphabetically among active members, ensuring they share the partition load without duplicate processing. Consumers read their assigned partitions and periodically commit their progress to the offset store via `ReqOffsetCommit` ([offset_manager.go](file:///c:/Users/yashn/KafkaLite/internal/consumer/offset_manager.go)), which flushes progress to disk with `fsync` for complete state recovery."*
