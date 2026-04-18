// partitioner.js — Lab 6: Partition assignment logic
'use strict';

function hashString(str) {
  let hash = 5381;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) + hash) + str.charCodeAt(i);
    hash = hash & hash;
  }
  return Math.abs(hash);
}

function assignPartition(key, numPartitions, counter = 0) {
  if (key !== undefined && key !== null) {
    const keyStr = typeof key === 'string' ? key : JSON.stringify(key);
    return hashString(keyStr) % numPartitions;
  }
  return counter % numPartitions;
}

module.exports = { assignPartition, hashString };
