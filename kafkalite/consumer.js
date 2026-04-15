// consumer.js — Lab 5: Stateful consumer with offset tracking
'use strict';

const net = require('net');

const GROUP  = process.argv[2] || 'default-group';
const TOPIC  = process.argv[3] || 'orders';
const POLL_MS = 500;
const COMMIT_EVERY = 5; // commit after every N records processed

let currentOffset = 0;
let processedSinceCommit = 0;
let buffer = '';
let client;

function connect() {
  client = net.connect({ port: 9092 }, () => {
    console.log(`[CONSUMER][${GROUP}] Connected. Looking up last committed offset for "${TOPIC}"...`);
    // Step 1: ask the broker where this group left off
    client.write(JSON.stringify({ cmd: 'GET_OFFSET', group: GROUP, topic: TOPIC }) + '\n');
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
  // Response to GET_OFFSET — start polling from resumeFrom
  if (data.resumeFrom !== undefined && !data.records) {
    currentOffset = data.resumeFrom;
    console.log(`[CONSUMER][${GROUP}] Last committed: ${data.committed}. Starting from offset ${currentOffset}`);
    startPolling();
    return;
  }

  // Response to FETCH — process records
  if (data.records) {
    for (const record of data.records) {
      console.log(`[CONSUMER][${GROUP}] offset=${record.offset}:`, record.value);
      currentOffset = record.offset + 1;
      processedSinceCommit++;
    }

    if (data.records.length > 0 && processedSinceCommit >= COMMIT_EVERY) {
      commitOffset(currentOffset - 1);
      processedSinceCommit = 0;
    }
    return;
  }

  // Response to COMMIT_OFFSET
  if (data.committed !== undefined) {
    console.log(`[CONSUMER][${GROUP}] Committed offset ${data.committed}`);
  }
}

function startPolling() {
  setInterval(() => {
    client.write(JSON.stringify({
      cmd: 'FETCH',
      topic: TOPIC,
      fromOffset: currentOffset,
      maxRecords: 10,
    }) + '\n');
  }, POLL_MS);
}

function commitOffset(offset) {
  client.write(JSON.stringify({
    cmd: 'COMMIT_OFFSET',
    group: GROUP,
    topic: TOPIC,
    offset,
  }) + '\n');
}

// Graceful shutdown: commit current position before exiting
process.on('SIGINT', () => {
  if (currentOffset > 0) {
    console.log(`\n[CONSUMER] Shutting down. Committing offset ${currentOffset - 1}`);
    commitOffset(currentOffset - 1);
    setTimeout(() => process.exit(0), 300);
  } else {
    process.exit(0);
  }
});

connect();
