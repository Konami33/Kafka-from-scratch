// broker.js — Lab 1: Basic TCP Server
'use strict';

const net = require('net');

const PORT = 9092; // Same default port as real Kafka (for familiarity)

const server = net.createServer((socket) => {
  const clientId = `${socket.remoteAddress}:${socket.remotePort}`;
  console.log(`[BROKER] Client connected: ${clientId}`);

  // Buffer to accumulate partial data (TCP streams don't guarantee full messages)
  let buffer = '';

  socket.on('data', (chunk) => {
    buffer += chunk.toString();

    // Process all complete newline-delimited messages in the buffer
    let newlineIndex;
    while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
      const rawMessage = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);

      if (!rawMessage) continue;

      try {
        const message = JSON.parse(rawMessage);
        handleMessage(socket, clientId, message);
      } catch (err) {
        sendResponse(socket, { status: 'error', message: 'Invalid JSON' });
      }
    }
  });

  socket.on('end', () => {
    console.log(`[BROKER] Client disconnected: ${clientId}`);
  });

  socket.on('error', (err) => {
    console.error(`[BROKER] Socket error for ${clientId}: ${err.message}`);
  });
});

function handleMessage(socket, clientId, message) {
  console.log(`[BROKER] Received from ${clientId}:`, message);

  // Lab 1: Just echo the message back with a status
  sendResponse(socket, {
    status: 'ok',
    echo: message,
    timestamp: Date.now(),
  });
}

function sendResponse(socket, data) {
  socket.write(JSON.stringify(data) + '\n');
}

server.listen(PORT, () => {
  console.log(`[BROKER] KafkaLite broker listening on port ${PORT}`);
});

server.on('error', (err) => {
  console.error(`[BROKER] Server error: ${err.message}`);
});
