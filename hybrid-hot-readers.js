"use strict";
(function() {

    load("jstests/noPassthrough/libs/index_build.js");

    const cacheSizeGB = 1;
    const docSize = 1024;
    const numDocs = Math.floor(0.9 * (cacheSizeGB * 1024 * 1024 * 1024 / docSize));

    jsTestLog("cacheSize: " + cacheSizeGB + ", numDocs: " + numDocs);

    let conn = MongoRunner.runMongod({wiredTigerCacheSizeGB: cacheSizeGB});

    const padding = 'x'.repeat(docSize);
    const numThreads = 8;

    const dbName = 'test';
    let testDB = conn.getDB(dbName);
    let hotColl = testDB.hotColl;
    hotColl.drop();

    let coldColl = testDB.coldColl;
    coldColl.drop();

    const bulkSize = 1000;
    for (let i = 0; i < numDocs / bulkSize; i++) {
        let bulk = coldColl.initializeUnorderedBulkOp();
        for (let j = 0; j < bulkSize; j++) {
            bulk.insert({indexField: (i * bulkSize + j), padding: padding});
        }
        assert.commandWorked(bulk.execute());
        print("Inserted docs: " + (i * bulkSize));
    }
    print("inserted documents into coldColl: " + numDocs);

    // Insert documents into hotColl such that they all fit in cache.
    for (let i = 0; i < numDocs / bulkSize; i++) {
        let bulk = hotColl.initializeUnorderedBulkOp();
        for (let j = 0; j < bulkSize; j++) {
            bulk.insert({_id: (i * bulkSize + j), padding: padding});
        }
        assert.commandWorked(bulk.execute());
        print("Inserted docs: " + (i * bulkSize));
    }
    print("inserted documents into hotColl: " + numDocs);

    // Start readers to fill up the cache.
    let buildFunc = function() {
        // Build index
        let startTime = Date.now();
        assert.commandWorked(db.coldColl.createIndex({indexField: 1}, {background: true}));
        let endTime = Date.now();
        jsTest.log("Index build took " + (endTime - startTime) + " ms.");
    };

    let shellAwait = startParallelShell(buildFunc, conn.port);
    print("started worker thread");

    const benchInterval = 1;
    let benchArgs = {
        db: dbName,
        ops:
            [{ns: hotColl.getFullName(), op: "findOne", query: {_id: {"#RAND_INT": [0, numDocs]}}}],
        parallel: numThreads,
        seconds: benchInterval,
        host: db.getMongo().host
    };

    let numIndexes = function(coll) {
        let res = coll.runCommand('listIndexes');
        assert.commandWorked(res);
        return res.cursor.firstBatch.length;
    };

    let totalOps = 0;
    let its = 0;
    while (numIndexes(coldColl) == 1) {
        let res = benchRun(benchArgs);
        totalOps += res.totalOps;
        its++;
    }
    jsTest.log("Performed " + totalOps + " ops in " + (its * benchInterval) +
               " seconds. avg ops/s: " + (totalOps / its * benchInterval));

    // Stop updater thread(s).
    shellAwait();
    print("stopped worker thread ");

    MongoRunner.stopMongod(conn);

})();
