// broker.js — Lab 6: Full partitioned broker
'use strict';

const net = require('net');
const storage = require('./storage');
const offsetStore = require('./offsetStore');
const { assignPartition } = require('./partitioner');

const PORT = 9092;
const DEFAULT_PARTITIONS = 3;

// { topicName: { numPartitions, partitionOffsets: [n, n, n] } }
const topicConfig = {};

// Round-robin counter per topic (for keyless publishing)
const rrCounters = {};

function recoverTopics() {
  const recovered = storage.loadAllTopics();
  for (const topicName of Object.keys(recovered)) {
    initTopic(topicName, DEFAULT_PARTITIONS, true);
  }
}

function initTopic(name, numPartitions = DEFAULT_PARTITIONS, fromDisk = false) {
  if (topicConfig[name]) return;

  const partitionOffsets = [];
  for (let p = 0; p < numPartitions; p++) {
    partitionOffsets.push(fromDisk ? storage.getNextOffset(name, p) : 0);
  }

  topicConfig[name] = { numPartitions, partitionOffsets };
  rrCounters[name] = 0;

  const action = fromDisk ? 'Recovered' : 'Created';
  console.log(`[BROKER] ${action} topic "${name}" with ${numPartitions} partitions`);
}

recoverTopics();

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
        handleMessage(socket, clientId, JSON.parse(raw));
      } catch (e) {
        sendTo(socket, { status: 'error', message: 'Invalid JSON' });
      }
    }
  });

  socket.on('end', () => console.log(`[BROKER] Client disconnected: ${clientId}`));
  socket.on('error', (err) => console.error(`[BROKER] Error: ${err.message}`));
});

// Declared async — Lab 7 adds await inside PUBLISH for acks=all.
// Without async, that await would be silently ignored (no error, wrong behaviour).
async function handleMessage(socket, clientId, msg) {
  switch (msg.cmd) {

    case 'CREATE_TOPIC': {
      const { topic, numPartitions = DEFAULT_PARTITIONS } = msg;
      if (!topic) return sendTo(socket, { status: 'error', message: 'CREATE_TOPIC requires topic' });
      initTopic(topic, numPartitions);
      sendTo(socket, { status: 'ok', topic, numPartitions });
      break;
    }

    case 'PUBLISH': {
      const { topic, value, key } = msg;
      if (!topic || value === undefined) {
        return sendTo(socket, { status: 'error', message: 'PUBLISH requires topic and value' });
      }

      initTopic(topic);

      const config = topicConfig[topic];
      const partition = assignPartition(key, config.numPartitions, rrCounters[topic]++);
      const offset = config.partitionOffsets[partition]++;

      const record = { offset, partition, topic, key: key || null, value, timestamp: Date.now() };
      storage.appendRecord(topic, record, partition);

      console.log(`[BROKER] PUBLISH -> "${topic}" partition=${partition} offset=${offset} key=${key || 'null'}`);
      sendTo(socket, { status: 'ok', topic, partition, offset });
      break;
    }

    case 'FETCH': {
      const { topic, partition = 0, fromOffset = 0, maxRecords = 100, group } = msg;
      if (!topic) return sendTo(socket, { status: 'error', message: 'FETCH requires topic' });

      initTopic(topic);

      // If group is provided and fromOffset is not, auto-resume from committed offset
      let startOffset = fromOffset;
      if (group && msg.fromOffset === undefined) {
        const committed = offsetStore.getCommittedOffset(group, topic, partition);
        startOffset = committed === -1 ? 0 : committed + 1;
      }

      const records = storage.readRecords(topic, startOffset, partition).slice(0, maxRecords);
      sendTo(socket, { status: 'ok', topic, partition, records, count: records.length });
      break;
    }

    case 'COMMIT_OFFSET': {
      const { group, topic, offset, partition = 0 } = msg;
      if (!group || !topic || offset === undefined) {
        return sendTo(socket, { status: 'error', message: 'COMMIT_OFFSET requires group, topic, offset' });
      }
      offsetStore.commitOffset(group, topic, offset, partition);
      sendTo(socket, { status: 'ok', committed: offset, partition });
      break;
    }

    case 'GET_OFFSET': {
      const { group, topic, partition = 0 } = msg;
      if (!group || !topic) {
        return sendTo(socket, { status: 'error', message: 'GET_OFFSET requires group and topic' });
      }
      const committed = offsetStore.getCommittedOffset(group, topic, partition);
      const resumeFrom = committed === -1 ? 0 : committed + 1;
      sendTo(socket, { status: 'ok', committed, resumeFrom, partition });
      break;
    }

    case 'LIST_TOPICS': {
      const info = Object.entries(topicConfig).map(([name, cfg]) => ({
        name,
        numPartitions: cfg.numPartitions,
        partitionOffsets: cfg.partitionOffsets,
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
