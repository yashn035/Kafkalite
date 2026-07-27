import socket
import struct

def consume(addr, topic, offset, max_bytes):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(addr)

    topic_bytes = topic.encode('utf-8')
    topic_prefix = struct.pack('>H', len(topic_bytes))
    offset_bytes = struct.pack('>q', offset)
    max_bytes_prefix = struct.pack('>i', max_bytes)

    # Combine body payload: ReqConsume = 2
    body = b'\x02' + topic_prefix + topic_bytes + offset_bytes + max_bytes_prefix
    header = struct.pack('>I', len(body))
    s.sendall(header + body)

    # Read 4-byte response length
    resp_len_buf = s.recv(4)
    if len(resp_len_buf) < 4:
        print("Failed to read header")
        return
    resp_len = struct.unpack('>I', resp_len_buf)[0]
    
    # Read full body
    data = b''
    while len(data) < resp_len:
        chunk = s.recv(resp_len - len(data))
        if not chunk:
            break
        data += chunk

    # Response format:
    # 1-byte status
    status = data[0]
    if status != 0:
        print("Error from broker")
        return

    # 8-byte next offset
    next_offset = struct.unpack('>q', data[1:9])[0]
    
    # 4-byte record count
    record_count = struct.unpack('>I', data[9:13])[0]
    print(f"Consumed {record_count} records. Next offset: {next_offset}")

    # Parse records:
    # Each record: 8-byte offset, 4-byte key len, key, 4-byte val len, val
    pos = 13
    for _ in range(record_count):
        rec_offset = struct.unpack('>q', data[pos:pos+8])[0]
        pos += 8
        k_len = struct.unpack('>I', data[pos:pos+4])[0]
        pos += 4
        key = data[pos:pos+k_len]
        pos += k_len
        v_len = struct.unpack('>I', data[pos:pos+4])[0]
        pos += 4
        val = data[pos:pos+v_len]
        pos += v_len
        print(f"  [{rec_offset}] key: {key.decode()}, val: {val.decode()}")
    s.close()

if __name__ == "__main__":
    consume(('localhost', 9092), 'test', 0, 65536)
