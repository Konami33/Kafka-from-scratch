// consumer.js — Lab 3: Poll a named topic
'use strict';

const net = require('net');

const topic = process.argv[2] || 'orders';
const POLL_MS = 500;

let currentOffset = 0;
let buffer = '';

const client = net.connect({ port: 9092 }, () => {
  console.log(`[CONSUMER] Polling topic "${topic}" every ${POLL_MS}ms`);
  startPolling();
});

function startPolling() {
  setInterval(() => {
    client.write(JSON.stringify({
      cmd: 'FETCH',
      topic,
      fromOffset: currentOffset,
      maxRecords: 10,
    }) + '\n');
  }, POLL_MS);
}

client.on('data', (chunk) => {
  buffer += chunk.toString();
  let idx;
  while ((idx = buffer.indexOf('\n')) !== -1) {
    const raw = buffer.slice(0, idx).trim();
    buffer = buffer.slice(idx + 1);
    if (!raw) continue;
    const data = JSON.parse(raw);
    if (data.records) {
      for (const record of data.records) {
        console.log(`[CONSUMER] [${record.topic}] offset=${record.offset}`, record.value);
        currentOffset = record.offset + 1;
      }
    }
  }
});

client.on('end', () => console.log('[CONSUMER] Disconnected'));
client.on('error', (err) => console.error('[CONSUMER] Error:', err.message));

process.on('SIGINT', () => { client.end(); process.exit(0); });
