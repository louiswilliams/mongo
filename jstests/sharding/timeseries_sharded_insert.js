/**
 * Tests that time-series collections can be sharded and that queries return correct results.
 *
 * @tags: [
 *   requires_fcv_49,
 *   requires_find_command,
 * ]
 */

(function() {
load("jstests/core/timeseries/libs/timeseries.js");
load("jstests/sharding/libs/find_chunks_util.js");

Random.setRandomSeed();

const st = new ShardingTest({shards: 2, rs: {nodes: 2}});

const dbName = 'test';
const sDB = st.s.getDB(dbName);
const configDB = st.s0.getDB('config');

if (!TimeseriesTest.timeseriesCollectionsEnabled(st.shard0)) {
    jsTestLog("Skipping test because the time-series collection feature flag is disabled");
    st.stop();
    return;
}

// Simple shard key on the metadata field.
(function metaShardKey() {
    assert.commandWorked(
        sDB.createCollection('ts', {timeseries: {timeField: 'time', metaField: 'hostId'}}));

    st.ensurePrimaryShard(dbName, st.shard0.shardName);

    // This index gets created as {meta: 1} on the buckets collection.
    const tsColl = st.shard0.getDB(dbName).ts;
    assert.commandWorked(tsColl.createIndex({hostId: 1}));

    assert.commandWorked(st.s.adminCommand({enableSharding: 'test'}));
    assert.commandWorked(
        st.s.adminCommand({shardCollection: 'test.system.buckets.ts', key: {'meta': 1}}));

    const numDocs = 20;
    let docs = [];
    for (let i = 0; i < numDocs; i++) {
        const doc = {
            time: ISODate(),
            hostId: i,
            _id: i,
            data: Random.rand(),
        };
        docs.push(doc);
        assert.commandWorked(sDB.ts.insert(doc));
    }

    // Split at 10
    assert.commandWorked(st.s.adminCommand({split: 'test.system.buckets.ts', middle: {meta: 10}}));

    // Migrate to the other shard
    let otherShard = st.getOther(st.getPrimaryShard(dbName)).name;
    assert.commandWorked(st.s.adminCommand({
        movechunk: 'test.system.buckets.ts',
        find: {meta: 10},
        to: otherShard,
        _waitForDelete: true
    }));

    let counts = st.chunkCounts('system.buckets.ts', 'test');
    assert.eq(1, counts[st.shard0.shardName]);
    assert.eq(1, counts[st.shard1.shardName]);

    // Insert more, now that there are chunks on different shards.
    for (let i = 0; i < numDocs; i++) {
        const doc = {
            time: ISODate(),
            hostId: i,
            _id: i,
            data: Random.rand(),
        };
        docs.push(doc);
        assert.commandWorked(sDB.ts.insert(doc));
    }

    // Query with shard key
    assert.docEq([docs[0], docs[20]], sDB.ts.find({hostId: 0}).toArray());
    assert.docEq([docs[19], docs[39]], sDB.ts.find({hostId: 19}).toArray());

    // Query without shard key
    assert.docEq(docs, sDB.ts.find().sort({time: 1}).toArray());

    assert.commandWorked(sDB.dropDatabase());
})();

st.stop();
})();
