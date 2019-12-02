/*!
 * Copyright (c) 2018-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _database = require('./database');
const _util = require('./util');
const workerpool = require('workerpool');
const _cacheKey = require('./cache-key');
const redis = require('promise-redis')();
const {MongoClient} = require('mongodb');
let cache;
let db;

const nodeState = new Map();

workerpool.worker({
  find: async ({
    cacheKey, chunk, config, databaseName, eventCollectionName, ledgerNodeId,
    operationCollectionName
  }) => {
    if(!cache) {
      cache = {client: redis.createClient({
        host: config.redis.host,
        port: config.redis.port,
      })};
    }
    if(!db) {
      const url = `mongodb://${config.mongodb.host}:${config.mongodb.port}`;
      const mongoClient = new MongoClient(url, {useUnifiedTopology: true});
      await mongoClient.connect();
      db = mongoClient.db(databaseName);
    }
    let state = nodeState.get(ledgerNodeId);
    if(!state) {
      state = {};
      state.cacheKey = cacheKey;
      state.eventCollection = db.collection(eventCollectionName);
      state.ledgerNodeId = ledgerNodeId;
      state.operationCollection = db.collection(operationCollectionName);
      nodeState.set(ledgerNodeId, state);
    }
    const rawEvents = await _getEvents(chunk);
    const processedData = _processEvents(rawEvents);
    const result = await _storeEvents({processedData, rangeData: chunk, state});
    return result;
  }
});

async function _getEvents(rangeData) {
  if(!rangeData || rangeData.length === 0) {
    throw new Error('A');
    // throw new BedrockError('Nothing to do.', 'AbortError');
  }
  const result = await cache.client.mget(rangeData);
  const eventsJson = [];
  for(const r of result) {
    // filter out nulls, nulls occur when an event was a duplicate
    if(r === null) {
      continue;
    }
    // throw if JSON.parse fails because there is a serious problem
    eventsJson.push(JSON.parse(r));
  }
  if(eventsJson.length === 0) {
    throw new Error('B');
    // throw new BedrockError('Nothing to do.', 'AbortError');
  }
  return eventsJson;
}

function _processEvents(rawEvents) {
  const now = Date.now();
  const events = [];
  const eventHashes = new Set();
  const _parentHashes = [];
  const headHashes = new Set();
  const creatorHeads = {};
  const operations = [];
  for(const {event, meta} of rawEvents) {
    const {eventHash} = meta;

    eventHashes.add(eventHash);
    _.defaults(meta, {created: now, updated: now});
    events.push({event, meta});

    if(event.type === 'WebLedgerOperationEvent') {
      operations.push(...event.operationRecords);
      delete event.operationRecords;
    }

    // build a list of treeHashes that are not included in this batch
    if(meta.continuity2017.type === 'm') {
      const {creator: creatorId, generation} = meta.continuity2017;
      // capturing the *last* head for each creator
      creatorHeads[creatorId] = {eventHash, generation};
      headHashes.add(eventHash);
      _parentHashes.push(...event.parentHash);
    }
  }
  // FIXME: remove
  // const parentHashes = _.uniq(_parentHashes);
  return {
    creatorHeads, eventHashes, events, headHashes, operations,
    parentHashes: _parentHashes
  };
}

async function _addEvents({events, state}) {
  const dupHashes = [];

  const chunks = _util.chunkDocuments(events);
  for(let chunk of chunks) {
    // retry indefinitely on duplicate errors (duplicates will be removed
    // from the `chunk` array so the rest of the events can be inserted)
    while(chunk.length > 0) {
      try {
        await state.eventCollection.insertMany(chunk, {ordered: true});
        chunk = [];
      } catch(e) {
        if(!_database.isDuplicateError(e)) {
          throw e;
        }
        // remove events up to the dup and retry
        dupHashes.push(chunk[e.index].meta.eventHash);
        chunk = chunk.slice(e.index + 1);
      }
    }
  }
  return {dupHashes};
}

async function _addOperations({ignoreDuplicate = true, operations, state}) {
  const chunks = _util.chunkDocuments(operations);
  for(const chunk of chunks) {
    try {
      await state.operationCollection.insertMany(chunk, {ordered: false});
    } catch(e) {
      if(ignoreDuplicate && _database.isDuplicateError(e)) {
        return;
      }
      throw e;
    }
  }
}

async function _storeEvents({processedData, state}) {
  const {
    creatorHeads, events, eventHashes, headHashes, operations, parentHashes
  } = processedData;
  if(operations.length !== 0) {
    // logger.debug(`Attempting to store ${operations.length} operations.`);
    await _addOperations({operations, state});
  }
  // logger.debug(`Attempting to store ${events.length} events.`);
  // retry on duplicate events until all events have been processed
  const {dupHashes} = await _addEvents({events, state});
  // logger.debug('Successfully stored events and operations.');
  const {ledgerNodeId} = state;
  const eventCountCacheKey = _cacheKey.eventCountPeer({
    ledgerNodeId,
    second: Math.round(Date.now() / 1000)
  });
  const multi = cache.client.multi();
  multi.incrby(eventCountCacheKey, events.length);

  // FIXME:
  // multi.expire(eventCountCacheKey, this.eventsConfig.counter.ttl);
  multi.expire(eventCountCacheKey, 10000);

  multi.srem(state.cacheKey.eventQueueSet, Array.from(eventHashes));

  await multi.exec();

  return {creatorHeads, dupHashes, headHashes: [...headHashes], parentHashes};
}
