const net = require('net');

function produce(host, port, topic, key, value) {
    const client = new net.Socket();
    client.connect(port, host, () => {
        const topicBuf = Buffer.from(topic, 'utf-8');
        const keyBuf = Buffer.from(key);
        const valBuf = Buffer.from(value);

        // Calculate request body size:
        // 1 byte request type + 2 bytes topic len + topic + 4 bytes key len + key + 4 bytes val len + val
        const bodySize = 1 + 2 + topicBuf.length + 4 + keyBuf.length + 4 + valBuf.length;
        const frame = Buffer.alloc(4 + bodySize);

        let offset = 0;
        frame.writeUInt32BE(bodySize, offset);
        offset += 4;
        frame.writeUInt8(1, offset); // ReqProduce = 1
        offset += 1;
        frame.writeUInt16BE(topicBuf.length, offset);
        offset += 2;
        topicBuf.copy(frame, offset);
        offset += topicBuf.length;
        frame.writeUInt32BE(keyBuf.length, offset);
        offset += 4;
        keyBuf.copy(frame, offset);
        offset += keyBuf.length;
        frame.writeUInt32BE(valBuf.length, offset);
        offset += 4;
        valBuf.copy(frame, offset);

        client.write(frame);
    });

    client.on('data', (data) => {
        if (data.length < 4) return;
        const respLen = data.readUInt32BE(0);
        const body = data.subarray(4, 4 + respLen);
        const status = body.readUInt8(0);
        if (status === 0) {
            const offsetVal = body.readBigInt64BE(1);
            console.log(`Produced! Status: OK, Offset: ${offsetVal}`);
        } else {
            console.log("Produce failed");
        }
        client.destroy();
    });
}

produce('localhost', 9092, 'test', 'jskey', 'jsval');
