// admin.js — Lab 3: Inspect broker state (Labs 3–5 version)
'use strict';

const net = require('net');

const client = net.connect({ port: 9092 }, () => {
  client.write(JSON.stringify({ cmd: 'LIST_TOPICS' }) + '\n');
});

let buffer = '';
client.on('data', (chunk) => {
  buffer += chunk.toString();
  let idx;
  while ((idx = buffer.indexOf('\n')) !== -1) {
    const raw = buffer.slice(0, idx).trim();
    buffer = buffer.slice(idx + 1);
    if (!raw) continue;
    const data = JSON.parse(raw);
    console.log('\n[ADMIN] Topics:');
    (data.topics || []).forEach(t => {
      console.log(`  - ${t.name}: ${t.messageCount} messages`);
    });
    client.end();
  }
});

client.on('error', (err) => console.error('[ADMIN] Error:', err.message));
