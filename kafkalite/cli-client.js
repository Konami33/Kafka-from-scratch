// cli-client.js — Lab 1: Interactive CLI client (reused in all labs)
'use strict';

const net = require('net');
const readline = require('readline');

const client = net.connect({ port: 9092 }, () => {
  console.log('[CLI] Connected to broker. Type a JSON command and press Enter.');
  console.log('[CLI] Example: {"cmd":"ping","data":"hello"}');
  console.log('[CLI] Type "exit" to disconnect.\n');
});

// --- Broker response handling ---
let buffer = '';
client.on('data', (chunk) => {
  buffer += chunk.toString();
  let idx;
  while ((idx = buffer.indexOf('\n')) !== -1) {
    const raw = buffer.slice(0, idx).trim();
    buffer = buffer.slice(idx + 1);
    if (raw) {
      try {
        const response = JSON.parse(raw);
        if (response.records) {
          console.log('[BROKER] topic:', response.topic, 'count:', response.count);
          for (const record of response.records) {
            console.log('  offset:', record.offset, 'value:', record.value);
          }
        } else {
          console.log('[BROKER]', response);
        }
      } catch {
        console.log('[BROKER] (raw):', raw);
      }
    }
  }
});

client.on('end', () => {
  console.log('[CLI] Disconnected from broker.');
  process.exit(0);
});

client.on('error', (err) => {
  console.error('[CLI] Connection error:', err.message);
  process.exit(1);
});

// --- Stdin handling ---
const rl = readline.createInterface({ input: process.stdin, output: process.stdout });

rl.on('line', (line) => {
  const trimmed = line.trim();
  if (!trimmed) return;
  if (trimmed.toLowerCase() === 'exit') {
    client.end();
    return;
  }
  client.write(trimmed + '\n');
});

rl.on('close', () => client.end());

process.on('SIGINT', () => {
  console.log('\n[CLI] Caught interrupt, closing...');
  client.end();
});
