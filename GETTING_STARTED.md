# Getting Started with KafkaLite

This 5-minute quick start guide will get you up and running with KafkaLite's core and enterprise features.

## 1. Run the Broker

You can launch a full cluster using Docker Compose:

```bash
# This will build and spin up the Brokers, API Gateway, and UI.
docker-compose up -d --build
```

Access the Web UI at **http://localhost:8082**.
*Note: If prompted for an auth token, use the `auth-cli` to generate one, or check the terminal logs.*

## 2. Produce & Consume via UI

1. Open the UI and navigate to the **Produce** tab.
2. Enter a Topic (e.g., `test-topic`) and a Value (e.g., `Hello KafkaLite!`).
3. Click "Send Message".
4. Navigate to the **Consume** tab, enter `test-topic`, and click "Fetch" to see your message.

## 3. Test Exactly-Once Semantics

KafkaLite supports Exactly-Once Processing to prevent duplicates.
1. In the **Produce** tab, fill out a Message and assign a `Message ID` (e.g., `msg-123`) and a `Producer ID` (e.g., `1`).
2. Click "Send Message". It will succeed.
3. Click "Send Message" *again* with the exact same IDs. The broker will acknowledge the message, but it will *not* be duplicated in the log.
4. Verify by consuming the topic—you will only see one message!

## 4. View AI Insights

1. Navigate to the **AI Insights** tab in the sidebar.
2. Click **Analyze**.
3. The dashboard will query the broker's real-time metrics and display human-readable health advice (e.g., "CPU Load is nominal", or warnings if you are pushing too much throughput).

## 5. Schema Registry & Auto-Rebalance

- **Schema Registry**: Try defining a JSON schema for a topic in the Schema Registry tab. Any subsequent produce requests that don't match the schema will be rejected!
- **Auto-Rebalance**: Start a large benchmark (`make bench`). The background rebalancer will detect the high load and automatically shift partition leaders to other brokers in the cluster.

Happy streaming!
