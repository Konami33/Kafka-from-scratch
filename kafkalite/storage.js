// storage.js - Simple file-based storage for KafkaLite
'use strict';

const fs = require('fs');
const path = require('path');

const DATA_ROOT = process.env.KAFKALITE_DATA_DIR || process.cwd();
const DATA_DIR  = path.join(DATA_ROOT, 'data', 'topics');

function getPartitionDir(topic, partition = 0) {
  return path.join(DATA_DIR, topic, `partition-${partition}`);
}

function getLogFilePath(topic, partition = 0) {
  return path.join(getPartitionDir(topic, partition), '00000000.log');
}

function ensurePartitionDir(topic, partition = 0) {
  const dir = getPartitionDir(topic, partition);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function appendRecord(topic, record, partition = 0) {
  ensurePartitionDir(topic, partition);
  const logPath = getLogFilePath(topic, partition);
  fs.appendFileSync(logPath, JSON.stringify(record) + '\n', 'utf8');
}

function readRecords(topic, fromOffset = 0, partition = 0) {
  const logPath = getLogFilePath(topic, partition);
  if (!fs.existsSync(logPath)) return [];

  const content = fs.readFileSync(logPath, 'utf8');
  const lines = content.split('\n').filter(l => l.trim());

  const records = [];
  for (const line of lines) {
    try {
      const record = JSON.parse(line);
      if (record.offset >= fromOffset) records.push(record);
    } catch (e) {
      console.error('[STORAGE] Corrupt record skipped:', line);
    }
  }
  return records;
}

function getNextOffset(topic, partition = 0) {
  const logPath = getLogFilePath(topic, partition);
  if (!fs.existsSync(logPath)) return 0;
  const content = fs.readFileSync(logPath, 'utf8');
  return content.split('\n').filter(l => l.trim()).length;
}

function loadAllTopics() {
  const result = {};
  if (!fs.existsSync(DATA_DIR)) return result;

  const topicDirs = fs.readdirSync(DATA_DIR);
  for (const topic of topicDirs) {
    const nextOffset = getNextOffset(topic);
    result[topic] = nextOffset;
    console.log(`[STORAGE] Recovered topic "${topic}" with ${nextOffset} records`);
  }
  return result;
}

module.exports = { appendRecord, readRecords, getNextOffset, loadAllTopics };