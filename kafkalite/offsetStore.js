// offsetStore.js — Lab 5: Durable offset tracking
'use strict';

const fs = require('fs');
const path = require('path');

const DATA_ROOT   = process.env.KAFKALITE_DATA_DIR || process.cwd();
const OFFSETS_DIR = path.join(DATA_ROOT, 'offsets');

function ensureDir() {
  fs.mkdirSync(OFFSETS_DIR, { recursive: true });
}

function offsetKey(group, topic, partition = 0) {
  const safe = s => s.replace(/[^a-zA-Z0-9_-]/g, '_');
  return path.join(OFFSETS_DIR, `${safe(group)}__${safe(topic)}__${partition}.json`);
}

function commitOffset(group, topic, offset, partition = 0) {
  ensureDir();
  const filePath = offsetKey(group, topic, partition);
  fs.writeFileSync(
    filePath,
    JSON.stringify({ group, topic, partition, offset, updatedAt: Date.now() }),
    'utf8'
  );
}

function getCommittedOffset(group, topic, partition = 0) {
  const filePath = offsetKey(group, topic, partition);
  if (!fs.existsSync(filePath)) return -1;
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8')).offset;
  } catch (e) {
    return -1;
  }
}

function listAllOffsets() {
  ensureDir();
  const files = fs.readdirSync(OFFSETS_DIR).filter(f => f.endsWith('.json'));
  return files.map(f => {
    try {
      return JSON.parse(fs.readFileSync(path.join(OFFSETS_DIR, f), 'utf8'));
    } catch (e) { return null; }
  }).filter(Boolean);
}

module.exports = { commitOffset, getCommittedOffset, listAllOffsets };
