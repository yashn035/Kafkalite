# Demo Walkthrough Video Script

A 5-MINUTE SCREEN-RECORDING DEMO FOR PORTFOLIO AND INTERVIEW SHOWCASE

---

## 🎬 Part 1: Project Setup and Overview (0:00 - 1:00)
* **Visuals**: Show your IDE with the directory structure of `KafkaLite` visible. Open the `README.md` file.
* **Script**:
  > *"Hello, today I will demonstrate KafkaLite, a high-performance distributed commit log engine I built in Go from first principles. It implements segment-based storage, logical sparse indexing, leader-follower replication, and consumer group rebalancing. Here is our project structure: we have custom storage segments in `internal/storage/segment.go`, custom TCP wire frames in `internal/protocol/protocol.go`, and leader election controller loops in `internal/broker/controller.go`. Let's compile the project."*
* **Action**: Run `make build` in the terminal to show compiling.
  > *"As you can see, the project compiles cleanly into standalone broker and client binaries inside the `./bin` directory."*

---

## 🎬 Part 2: Running the Automated Failover Demo (1:00 - 2:30)
* **Visuals**: Run the command `make demo` in the terminal.
* **Script**:
  > *"To show the cluster's high availability, I will run the automated demo target: `make demo`. This command spins up a 3-broker topology using Docker Compose, mounts a shared data volume, and waits for our HTTP health-check endpoints to report alive."*
* **Action**: Scroll the terminal output as Docker Compose builds and starts the containers, showing the curls to `:8081/health` succeeding.
  > *"Once the cluster registers healthy, the producer client connects to Broker-0 on port 9092 and publishes 1,000 initial messages. At the same time, we start Consumer A and Consumer B under the consumer group 'my-group'. The coordinator rebalances partition 0 and partition 1, split-assigning them across both consumers."*

---

## 🎬 Part 3: Simulating Leader Failover (2:30 - 3:45)
* **Visuals**: Highlight the logs showing `docker stop kafkalite-broker-0`.
* **Script**:
  > *"Now, we simulate a leader failure. The script executes a docker stop on `broker-0` which was the leader for partition 0. Within 5 seconds, the background controller polling loop on `broker-1` detects that the leader is dead. It atomically acquires a file lock on `leaders.json.lock` over the shared volume, reassigns partition 0 to itself, and logs the failover state."*
* **Action**: Open `consumer_a.log` or highlight the logs showing `Failover triggered: partition test-0 now led by broker 1`.
  > *"Notice that the producer client automatically shifts its connection target to port 9093, producing 500 more messages without failing or losing data. The remaining brokers handle the consumer group rebalancing seamlessly, keeping the reads active."*

---

## 🎬 Part 4: Exposing Observability & Prometheus Metrics (3:45 - 4:15)
* **Visuals**: Open a browser window to `http://localhost:8080/metrics`.
* **Script**:
  > *"We track broker internals using Prometheus metrics exposed on port 8080. Here we have live metrics registers mapping message rates, read-write bytes, partition sizes, and execution write latencies. The administration endpoint on port 8081 also exposes the active broker health mappings."*

---

## 🎬 Part 5: Benchmark Mode & Wrap Up (4:15 - 5:00)
* **Visuals**: Run the command `make bench` in the terminal.
* **Script**:
  > *"Finally, we run our dedicated benchmark mode using `make bench`. This publishes 100,000 messages directly over the socket and records request durations. As you can see, we achieve a write throughput of over 106,000 messages per second, with a p99 write latency of just 1.23 milliseconds on this system."*
* **Action**: Highlight the console output table containing latency percentiles.
  > *"This concludes the walkthrough. KafkaLite shows that we can build highly durable, concurrent, and self-healing distributed engines with clean separation of concerns and zero-dependency coordination. Thank you."*
