import socket
import struct
import threading

# Partition storage: maps topic to a list of dicts: {'offset': int, 'key': bytes, 'val': bytes}
storage = {}
storage_lock = threading.Lock()

def handle_client(conn, addr):
    print(f"[Broker] Client connected from {addr}")
    try:
        while True:
            # Read 4-byte frame length prefix
            len_buf = conn.recv(4)
            if len(len_buf) < 4:
                break
            payload_len = struct.unpack('>I', len_buf)[0]
            
            # Read the full frame payload
            data = b''
            while len(data) < payload_len:
                chunk = conn.recv(payload_len - len(data))
                if not chunk:
                    break
                data += chunk
            if len(data) < payload_len:
                break

            req_type = data[0]
            if req_type == 1:  # ReqProduce
                # Parse Topic (string: 2-byte len + data)
                topic_len = struct.unpack('>H', data[1:3])[0]
                pos = 3
                topic = data[pos:pos+topic_len].decode('utf-8')
                pos += topic_len

                # Parse Key (binary: 4-byte len + data)
                key_len = struct.unpack('>I', data[pos:pos+4])[0]
                pos += 4
                key = data[pos:pos+key_len]
                pos += key_len

                # Parse Value (binary: 4-byte len + data)
                val_len = struct.unpack('>I', data[pos:pos+4])[0]
                pos += 4
                value = data[pos:pos+val_len]

                # Store record
                with storage_lock:
                    if topic not in storage:
                        storage[topic] = []
                    offset = len(storage[topic])
                    storage[topic].append({'offset': offset, 'key': key, 'val': value})

                print(f"[Broker] Produced to '{topic}': key={key.decode()}, val={value.decode()} -> assigned offset={offset}")

                # Send Response: Status (1-byte OK=0), Offset (8-byte int64)
                body = b'\x00' + struct.pack('>q', offset)
                header = struct.pack('>I', len(body))
                conn.sendall(header + body)

            elif req_type == 2:  # ReqConsume
                # Parse Topic (string: 2-byte len + data)
                topic_len = struct.unpack('>H', data[1:3])[0]
                pos = 3
                topic = data[pos:pos+topic_len].decode('utf-8')
                pos += topic_len

                # Parse Offset (8-byte int64)
                req_offset = struct.unpack('>q', data[pos:pos+8])[0]
                pos += 8

                # Parse MaxBytes (4-byte int32)
                max_bytes = struct.unpack('>i', data[pos:pos+4])[0]

                # Fetch records
                records_to_send = []
                with storage_lock:
                    if topic in storage:
                        for rec in storage[topic]:
                            if rec['offset'] >= req_offset:
                                records_to_send.append(rec)

                # Format response payload
                # Status (1-byte OK=0)
                status = b'\x00'
                
                # Format records list: count (4-byte uint32), followed by each record
                rec_payload = b''
                next_offset = req_offset
                count = 0
                for rec in records_to_send:
                    # Each record: 8-byte offset, 4-byte key len, key, 4-byte val len, val
                    rec_buf = struct.pack('>q', rec['offset'])
                    rec_buf += struct.pack('>I', len(rec['key'])) + rec['key']
                    rec_buf += struct.pack('>I', len(rec['val'])) + rec['val']
                    if len(rec_payload) + len(rec_buf) > max_bytes:
                        break
                    rec_payload += rec_buf
                    next_offset = rec['offset'] + 1
                    count += 1

                body = status + struct.pack('>q', next_offset) + struct.pack('>I', count) + rec_payload
                header = struct.pack('>I', len(body))
                conn.sendall(header + body)
                print(f"[Broker] Consumed from '{topic}': returned {count} records starting from offset {req_offset}")
            else:
                print(f"[Broker] Unknown request type: {req_type}")
                break
    except Exception as e:
        print(f"[Broker] Error: {e}")
    finally:
        conn.close()
        print(f"[Broker] Client disconnected from {addr}")

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('0.0.0.0', 9092))
    s.listen(5)
    print("[Broker] Mock Broker listening on TCP port 9092...")
    try:
        while True:
            conn, addr = s.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr))
            t.daemon = True
            t.start()
    except KeyboardInterrupt:
        print("\n[Broker] Shutting down.")
    finally:
        s.close()

if __name__ == "__main__":
    main()
