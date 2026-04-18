// producer.js — Lab 6: Key-based partitioning
'use strict';

const net = require('net');

const topic = process.argv[2] || 'orders';
const count  = parseInt(process.argv[3]) || 9;
const keys   = ['user-A', 'user-B', 'user-C'];

const client = net.connect({ port: 9092 }, () => {
  console.log(`[PRODUCER] Publishing ${count} messages to "${topic}" with key routing`);

  for (let i = 0; i < count; i++) {
    setTimeout(() => {
      const key   = keys[i % keys.length];
      const value = { msgId: i + 1, key, data: `Event ${i + 1}` };
      console.log(`[PRODUCER] key="${key}" ->`, value);
      client.write(JSON.stringify({ cmd: 'PUBLISH', topic, key, value }) + '\n');
    }, i * 200);
  }

  setTimeout(() => client.end(), count * 200 + 300);
});

let buffer = '';
client.on('data', (chunk) => {
  buffer += chunk.toString();
  let idx;
  while ((idx = buffer.indexOf('\n')) !== -1) {
    const raw = buffer.slice(0, idx).trim();
    buffer = buffer.slice(idx + 1);
    if (raw) {
      const ack = JSON.parse(raw);
      console.log(`[PRODUCER] Ack: partition=${ack.partition} offset=${ack.offset}`);
    }
  }
});

client.on('error', (err) => console.error('[PRODUCER] Error:', err.message));
