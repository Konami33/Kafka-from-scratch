// broker.js — Lab 2: Pull-based producer/consumer
'use strict';

const net = require('net');

const PORT = 9092;

// In-memory log: { topicName: [record, record, ...] }
const topics = {};

const server = net.createServer((socket) => {
  const clientId = `${socket.remoteAddress}:${socket.remotePort}`;
  console.log(`[BROKER] Client connected: ${clientId}`);

  let buffer = '';

  socket.on('data', (chunk) => {
    buffer += chunk.toString();
    let idx;
    while ((idx = buffer.indexOf('\n')) !== -1) {
      const raw = buffer.slice(0, idx).trim();
      buffer = buffer.slice(idx + 1);
      if (!raw) continue;
      try {
        handleMessage(socket, JSON.parse(raw));
      } catch (e) {
        sendTo(socket, { status: 'error', message: 'Invalid JSON' });
      }
    }
  });

  socket.on('end', () => {
    console.log(`[BROKER] Client disconnected: ${clientId}`);
  });

  socket.on('error', (err) => {
    console.error(`[BROKER] Error [${clientId}]: ${err.message}`);
  });
});

function ensureTopic(name) {
  if (!topics[name]) {
    topics[name] = [];
    console.log(`[BROKER] Auto-created topic: "${name}"`);
  }
}

function handleMessage(socket, msg) {
  switch (msg.cmd) {

    case 'PUBLISH': {
      const { topic, value } = msg;
      if (!topic || value === undefined) {
        return sendTo(socket, { status: 'error', message: 'PUBLISH requires topic and value' });
      }

      ensureTopic(topic);

      const record = {
        offset: topics[topic].length,
        topic,
        value,
        timestamp: Date.now(),
      };

      topics[topic].push(record);
      console.log(`[BROKER] PUBLISH -> topic="${topic}" offset=${record.offset}`);

      // No fan-out. No subscriber list. Just return the offset.
      sendTo(socket, { status: 'ok', offset: record.offset, topic });
      break;
    }

    case 'FETCH': {
      // Consumer asks: "Give me messages from offset N onwards"
      const { topic, fromOffset = 0, maxRecords = 100 } = msg;
      if (!topic) {
        return sendTo(socket, { status: 'error', message: 'FETCH requires topic' });
      }

      ensureTopic(topic);

      const records = topics[topic]
        .filter(r => r.offset >= fromOffset)
        .slice(0, maxRecords);

      sendTo(socket, { status: 'ok', topic, records, count: records.length });
      break;
    }

    default:
      sendTo(socket, { status: 'error', message: `Unknown command: ${msg.cmd}` });
  }
}

function sendTo(socket, data) {
  if (!socket.destroyed) {
    socket.write(JSON.stringify(data) + '\n');
  }
}

server.listen(PORT, () => {
  console.log(`[BROKER] Listening on port ${PORT}`);
});

server.on('error', (err) => {
  console.error(`[BROKER] Server error: ${err.message}`);
});
