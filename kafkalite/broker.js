// broker.js — Lab 4: With persistent storage
'use strict';

const net = require('net');
const storage = require('./storage');

const PORT = 9092;

// In-memory offset counters (recovered from disk on startup)
const topicOffsets = {}; // { topicName: nextOffset }

// --- Startup: recover existing topics from disk ---
const recovered = storage.loadAllTopics();
for (const [topic, nextOffset] of Object.entries(recovered)) {
  topicOffsets[topic] = nextOffset;
}
console.log(`[BROKER] Recovered ${Object.keys(recovered).length} topics from disk`);

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

  socket.on('end', () => console.log(`[BROKER] Client disconnected: ${clientId}`));
  socket.on('error', (err) => console.error(`[BROKER] Socket error: ${err.message}`));
});

function ensureTopic(name) {
  if (topicOffsets[name] === undefined) {
    topicOffsets[name] = storage.getNextOffset(name);
    console.log(`[BROKER] Created topic: "${name}"`);
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
        offset: topicOffsets[topic]++,
        topic,
        value,
        timestamp: Date.now(),
      };

      // Write to disk BEFORE confirming to producer
      storage.appendRecord(topic, record);

      console.log(`[BROKER] PUBLISH -> "${topic}" [offset ${record.offset}]`);
      sendTo(socket, { status: 'ok', offset: record.offset, topic });
      break;
    }

    case 'FETCH': {
      // Pull-based fetch: reads directly from disk log
      const { topic, fromOffset = 0, maxRecords = 100 } = msg;
      if (!topic) return sendTo(socket, { status: 'error', message: 'FETCH requires topic' });

      ensureTopic(topic);
      const records = storage.readRecords(topic, fromOffset).slice(0, maxRecords);
      sendTo(socket, { status: 'ok', topic, records, count: records.length });
      break;
    }

    case 'LIST_TOPICS': {
      const info = Object.keys(topicOffsets).map(name => ({
        name,
        messageCount: topicOffsets[name],
      }));
      sendTo(socket, { status: 'ok', topics: info });
      break;
    }

    default:
      sendTo(socket, { status: 'error', message: `Unknown command: ${msg.cmd}` });
  }
}

function sendTo(socket, data) {
  if (!socket.destroyed) socket.write(JSON.stringify(data) + '\n');
}

server.listen(PORT, () => console.log(`[BROKER] Listening on port ${PORT}`));

server.on('error', (err) => console.error(`[BROKER] Server error: ${err.message}`));
