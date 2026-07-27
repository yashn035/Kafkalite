# KafkaLite Client Connection Guide

This guide details how to interact with the live deployed KafkaLite cluster on the cloud VM.

---

## 🚀 Producing Messages

Publish a batch of test messages to the VM instance (replace `<VM_IP>` with the public IP address printed during deployment):

```bash
# Produce 100 messages to the 'transactions' topic on Broker-0
go run cmd/client/main.go --broker <VM_IP>:9092 --mode produce --messages 100 --log
```

---

## 📥 Consuming Messages

Start consumer group balanced consumers reading partition data:

### Consumer A (dials Broker-1)
```bash
go run cmd/client/main.go --broker <VM_IP>:9093 --mode consume --topic transactions --group group-alpha --consumer-id A --log
```

### Consumer B (dials Broker-2)
```bash
go run cmd/client/main.go --broker <VM_IP>:9094 --mode consume --topic transactions --group group-alpha --consumer-id B --log
```

The cluster coordinator automatically rebalances partition `0` to Consumer A and partition `1` to Consumer B, split-delivering the load.

---

## 📊 Monitoring Internals

To view active logs, partition allocations, and throughput times:

* **Broker 0 Health Endpoint**: `http://<VM_IP>:8081/health`
* **Broker 0 Prometheus Metrics**: `http://<VM_IP>:8080/metrics`
