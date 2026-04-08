// producer.js — Lab 2
'use strict';

const net = require('net');

const client = net.connect({ port: 9092 }, () => {
  console.log('[PRODUCER] Connected to broker');

  const topic = 'orders';
  const messages = [
    { orderId: 1, item: 'Widget A', qty: 5 },
    { orderId: 2, item: 'Widget B', qty: 2 },
    { orderId: 3, item: 'Widget C', qty: 10 },
  ];

  messages.forEach((value, i) => {
    setTimeout(() => {
      console.log('[PRODUCER] Publishing:', value);
      client.write(JSON.stringify({ cmd: 'PUBLISH', topic, value }) + '\n');
    }, i * 500);
  });

  setTimeout(() => client.end(), messages.length * 500 + 200);
});

let buffer = '';
client.on('data', (chunk) => {
  buffer += chunk.toString();
  let idx;
  while ((idx = buffer.indexOf('\n')) !== -1) {
    const raw = buffer.slice(0, idx).trim();
    buffer = buffer.slice(idx + 1);
    if (raw) console.log('[PRODUCER] Broker ack:', JSON.parse(raw));
  }
});

client.on('error', (err) => console.error('[PRODUCER] Error:', err.message));
