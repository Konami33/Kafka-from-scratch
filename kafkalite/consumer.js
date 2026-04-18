// consumer.js — Lab 6: Poll a specific partition
'use strict';

const net = require('net');

const GROUP     = process.argv[2] || 'workers';
const TOPIC     = process.argv[3] || 'orders';
const PARTITION = parseInt(process.argv[4] || '0');
const POLL_MS   = 500;

let currentOffset = 0;
let buffer = '';
let client;

function connect() {
  client = net.connect({ port: 9092 }, () => {
    console.log(`[CONSUMER][${GROUP}] Connected. Looking up offset for "${TOPIC}:${PARTITION}"`);
    client.write(JSON.stringify({
      cmd: 'GET_OFFSET',
      group: GROUP,
      topic: TOPIC,
      partition: PARTITION,
    }) + '\n');
  });

  client.on('data', (chunk) => {
    buffer += chunk.toString();
    let idx;
    while ((idx = buffer.indexOf('\n')) !== -1) {
      const raw = buffer.slice(0, idx).trim();
      buffer = buffer.slice(idx + 1);
      if (!raw) continue;
      handleBrokerMessage(JSON.parse(raw));
    }
  });

  client.on('end', () => console.log('[CONSUMER] Disconnected'));
  client.on('error', (err) => console.error('[CONSUMER] Error:', err.message));
}

function handleBrokerMessage(data) {
  if (data.resumeFrom !== undefined && !data.records) {
    currentOffset = data.resumeFrom;
    console.log(`[CONSUMER][${GROUP}] Partition ${PARTITION}: resuming from offset ${currentOffset}`);
    startPolling();
    return;
  }

  if (data.records) {
    for (const record of data.records) {
      console.log(
        `[CONSUMER][${GROUP}][${record.topic}:${record.partition}] offset=${record.offset} key=${record.key}`,
        record.value
      );
      currentOffset = record.offset + 1;

      // Commit after each record for demo clarity
      client.write(JSON.stringify({
        cmd: 'COMMIT_OFFSET',
        group: GROUP,
        topic: TOPIC,
        offset: record.offset,
        partition: PARTITION,
      }) + '\n');
    }
  }
}

function startPolling() {
  setInterval(() => {
    client.write(JSON.stringify({
      cmd: 'FETCH',
      topic: TOPIC,
      partition: PARTITION,
      fromOffset: currentOffset,
      maxRecords: 10,
    }) + '\n');
  }, POLL_MS);
}

process.on('SIGINT', () => { client.end(); process.exit(0); });

connect();
