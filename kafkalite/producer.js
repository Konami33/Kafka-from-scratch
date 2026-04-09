// producer.js — Lab 3
'use strict';

const net = require('net');

const topic = process.argv[2] || 'orders';
const messageCount = parseInt(process.argv[3]) || 3;

const client = net.connect({ port: 9092 }, () => {
  console.log(`[PRODUCER] Publishing ${messageCount} messages to topic "${topic}"`);

  for (let i = 0; i < messageCount; i++) {
    setTimeout(() => {
      const value = { id: i + 1, topic, data: `Message ${i + 1}`, ts: Date.now() };
      client.write(JSON.stringify({ cmd: 'PUBLISH', topic, value }) + '\n');
    }, i * 300);
  }

  setTimeout(() => client.end(), messageCount * 300 + 200);
});

let buffer = '';
client.on('data', (chunk) => {
  buffer += chunk.toString();
  let idx;
  while ((idx = buffer.indexOf('\n')) !== -1) {
    const raw = buffer.slice(0, idx).trim();
    buffer = buffer.slice(idx + 1);
    if (raw) console.log('[PRODUCER] Ack:', JSON.parse(raw));
  }
});

client.on('error', (err) => console.error('[PRODUCER] Error:', err.message));
