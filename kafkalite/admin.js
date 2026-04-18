// admin.js — Lab 6+: Partition-aware topic listing
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
      console.log(`  Topic: ${t.name} (${t.numPartitions} partitions)`);
      for (let p = 0; p < t.numPartitions; p++) {
        console.log(`    partition-${p}: offset=${t.partitionOffsets[p]}`);
      }
    });
    client.end();
  }
});

client.on('error', (err) => console.error('[ADMIN] Error:', err.message));
