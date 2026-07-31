import time
import sys

# ANSI Colors
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
MAGENTA = "\033[95m"
BLUE = "\033[94m"
RESET = "\033[0m"

def print_log(sender, message, color=RESET):
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S")
    print(f"{color}[{timestamp}] {sender:<12} | {message}{RESET}")
    sys.stdout.flush()

def run_simulation():
    print(f"\n{CYAN}===================================================================={RESET}")
    print(f"{CYAN}             KAFKALITE INTERACTIVE RUN SIMULATOR                    {RESET}")
    print(f"{CYAN}===================================================================={RESET}\n")
    
    time.sleep(1.0)
    print_log("SYSTEM", "Initializing 3-node broker topology...", CYAN)
    time.sleep(0.8)
    print_log("Broker-0", "Starting TCP broker on 0.0.0.0:9092 (ID=0)...", GREEN)
    print_log("Broker-0", "Loading partition metadata... test-0 led by ID=0", GREEN)
    time.sleep(0.5)
    print_log("Broker-1", "Starting TCP broker on 0.0.0.0:9093 (ID=1)...", GREEN)
    print_log("Broker-1", "Loading partition metadata... test-1 led by ID=1", GREEN)
    time.sleep(0.5)
    print_log("Broker-2", "Starting TCP broker on 0.0.0.0:9094 (ID=2)...", GREEN)
    print_log("Broker-2", "Syncing replication loops from Leader Broker-0...", GREEN)
    time.sleep(1.0)
    print_log("SYSTEM", "Broker cluster online and fully sync'd. Admin metrics live on port 8080.", CYAN)
    
    time.sleep(1.5)
    print(f"\n{YELLOW}--- Starting Consumer Group Rebalancing ---{RESET}\n")
    time.sleep(0.8)
    print_log("Coordinator", "Received ReqJoinGroup from Client-A (MemberID: host-1025, Subscriptions: [test])", MAGENTA)
    print_log("Coordinator", "Received ReqJoinGroup from Client-B (MemberID: host-1026, Subscriptions: [test])", MAGENTA)
    time.sleep(1.0)
    print_log("Coordinator", "Generation 1 triggered. Rebalancing topic 'test' (2 partitions) using RangeAssignor...", MAGENTA)
    time.sleep(0.8)
    print_log("Client-A", "Assigned partitions: {test: [0]}", BLUE)
    print_log("Client-B", "Assigned partitions: {test: [1]}", BLUE)
    print_log("Client-A", "Starting fetch loop from Broker-0 (partition 0) at offset 0", BLUE)
    print_log("Client-B", "Starting fetch loop from Broker-1 (partition 1) at offset 0", BLUE)
    
    time.sleep(1.5)
    print(f"\n{YELLOW}--- Producer Load Test (Writing to Partition Leaders) ---{RESET}\n")
    time.sleep(0.8)
    for i in range(1, 6):
        key = f"key-{i}"
        val = f"transaction-record-{i*100}"
        print_log("Producer", f"Sending record {i}/5 to Broker-0 (topic test, partition 0)...", YELLOW)
        time.sleep(0.3)
        print_log("Broker-0", f"Append log segment test-0: written '{key}':'{val}' (logical offset={i-1})", GREEN)
        print_log("Broker-0", "Fsync committed to disk. Replicating message payload to follower Broker-2...", GREEN)
        time.sleep(0.2)
        print_log("Broker-2", f"Replication ACK: copied offset {i-1} from Broker-0", GREEN)
        print_log("Producer", f"Received write response: Status=OK, Partition=0, Offset={i-1}", YELLOW)
        time.sleep(0.4)
        print_log("Client-A", f"Fetched record: key={key}, value={val}, offset={i-1} (committing progress)", BLUE)
        time.sleep(0.2)
        print_log("Broker-0", f"Committing group group-alpha offset partition 0 -> {i}", GREEN)
        print(f"  +- {GREEN}Prometheus Metric Updated: kafkalite_messages_produced_total += 1{RESET}")
        time.sleep(0.6)

    time.sleep(1.5)
    print(f"\n{RED}===================================================================={RESET}")
    print(f"{RED}         SIMULATING LEADER FAILURE: STOPPING BROKER-0 (ID=0)        {RESET}")
    print(f"{RED}===================================================================={RESET}\n")
    time.sleep(1.0)
    print_log("SYSTEM", "Killing Broker-0 container...", RED)
    time.sleep(0.8)
    print_log("Broker-1", "Controller: Health check to Broker-0:9092 failed. Node unreachable.", GREEN)
    print_log("Broker-1", "Controller: Attempting lock acquisition on /data/metadata/leaders.json.lock...", GREEN)
    time.sleep(0.5)
    print_log("Broker-1", "Controller: Lock acquired atomically. Initiating failover workflow...", GREEN)
    time.sleep(0.8)
    print_log("Broker-1", "Controller: Swapping partition test-0 leader mapping: 0 -> 1", GREEN)
    print_log("Broker-1", "Controller: Failover completed. Partition test-0 now led by Broker-1.", GREEN)
    
    time.sleep(1.5)
    print(f"\n{YELLOW}--- Producer Reconnects and Resumes Writing ---{RESET}\n")
    time.sleep(0.8)
    print_log("Producer", "Write failed on port 9092. Fetching updated metadata catalog...", YELLOW)
    time.sleep(0.5)
    print_log("Producer", "Metadata loaded. Partition test-0 is now led by port 9093. Redirecting write...", YELLOW)
    time.sleep(0.8)
    print_log("Producer", "Sending record 6/6 to Broker-1 (topic test, partition 0)...", YELLOW)
    time.sleep(0.3)
    print_log("Broker-1", "Append log segment test-0: written 'key-6':'transaction-record-600' (logical offset=5)", GREEN)
    print_log("Broker-1", "Fsync committed. Replicating payload to follower Broker-2...", GREEN)
    time.sleep(0.2)
    print_log("Broker-2", "Replication ACK: copied offset 5 from Broker-1", GREEN)
    print_log("Producer", "Received write response: Status=OK, Partition=0, Offset=5", YELLOW)
    
    time.sleep(1.5)
    print(f"\n{CYAN}===================================================================={RESET}")
    print(f"{CYAN}             KAFKALITE RUN SIMULATION COMPLETED                     {RESET}")
    print(f"{CYAN}===================================================================={RESET}\n")

if __name__ == "__main__":
    run_simulation()
