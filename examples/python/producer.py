import socket
import struct

def produce(addr, topic, key, value):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(addr)

    # String fields: 2-byte len + data
    topic_bytes = topic.encode('utf-8')
    topic_prefix = struct.pack('>H', len(topic_bytes))

    # Key / Value fields: 4-byte len + data
    key_prefix = struct.pack('>I', len(key))
    val_prefix = struct.pack('>I', len(value))

    # Combine body payload: ReqProduce = 1
    body = b'\x01' + topic_prefix + topic_bytes + key_prefix + key + val_prefix + value

    # 4-byte frame length prefix
    header = struct.pack('>I', len(body))
    s.sendall(header + body)

    # Read response: 4-byte length prefix
    resp_len_buf = s.recv(4)
    if len(resp_len_buf) < 4:
        print("Failed to read header")
        return
    resp_len = struct.unpack('>I', resp_len_buf)[0]
    resp_body = s.recv(resp_len)

    # Response starts with status (1-byte, OK = 0)
    status = resp_body[0]
    if status == 0:
        # Next is logical offset (8-byte int64)
        offset = struct.unpack('>q', resp_body[1:9])[0]
        print(f"Produced! Status: OK, Offset: {offset}")
    else:
        print("Produced failed with error status")
    s.close()

if __name__ == "__main__":
    produce(('localhost', 9092), 'test', b'pykey', b'pyval')
