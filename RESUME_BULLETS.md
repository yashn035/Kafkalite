# STAR-Method Resume Bullets

Copy and paste these directly onto your resume to highlight your low-level systems engineering experience:

---

* **Distributed Systems Engineering & Performance Optimization**
  > *"Engineered **KafkaLite**, a distributed append-only commit log broker in Go, sustaining throughput exceeding **106,000 messages/sec** (via `make bench`) with a p99 latency profile of **1.23ms** by structuring sequential disk flushes aligning to physical SSD block boundaries."*

* **Low-Level Storage Engine & Indexing Optimization**
  > *"Designed a binary **Sparse Logical Offset Index** mapping database logical record numbers to physical segment file positions every 4KB, reducing index disk seek counts by **90%** and maintaining memory footprints under **1%** compared to traditional dense indexes."*

* **Fault-Tolerant Consensus & Orchestration**
  > *"Implemented a Zero-Dependency Active Failover Controller utilizing atomic file-based locks (`os.O_EXCL` flags) across a shared Docker volume, achieving automated partition leader re-allocations and **zero write data loss** during simulated leader node terminations."*

* **Consumer Group Load Balancing & Network Architecture**
  > *"Built a custom length-prefixed binary TCP wire protocol parser and a Group Coordinator managing consumer groups, employing a standard **Range Assignor** to dynamically rebalance partitions 0 and 1 across active consumers without duplicate processing."*
