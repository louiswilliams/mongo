"use strict";
(function() {

    load("jstests/noPassthrough/libs/index_build.js");

    // Configuration
    const config = {
        background: false,
        cacheSizeGB: 2,
        dbpath: '/data/db/perf',
        docSize: 4000,
        docsPercentOfCache: 0.75,
        numThreads: 16,
        readOnce: true,

        // Only change these to 'true' when loading data for the first time.
        load: false,
        clean: false
    };

    jsTestLog(config);

    const kGB = 1024 * 1024 * 1024;
    const numDocs =
        Math.floor(config.docsPercentOfCache * (config.cacheSizeGB * kGB / config.docSize));

    jsTestLog("numDocs: " + numDocs);

    if (config.clean) {
        resetDbpath(config.dbpath);
    }

    let conn = MongoRunner.runMongod({
        dbpath: config.dbpath,
        noCleanData: !config.clean,
        wiredTigerCacheSizeGB: config.cacheSizeGB,
        setParameter: {useReadOnceCursorsForIndexBuilds: config.readOnce}
    });

    const padding = 'x'.repeat(config.docSize);

    // Different dbs so foreground builds to not compete with database locks
    const hotDbName = 'hot';
    const coldDbName = 'cold';
    let hotDB = conn.getDB(hotDbName);
    let coldDB = conn.getDB(coldDbName);

    let hotColl = hotDB.hotColl;
    let coldColl = coldDB.coldColl;

    // Load first
    if (config.load) {
        hotColl.drop();
        coldColl.drop();

        jsTestLog("Inserting into cold collection");
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
        jsTestLog("Inserting into hot collection");
        for (let i = 0; i < numDocs / bulkSize; i++) {
            let bulk = hotColl.initializeUnorderedBulkOp();
            for (let j = 0; j < bulkSize; j++) {
                bulk.insert({_id: (i * bulkSize + j), padding: padding});
            }
            assert.commandWorked(bulk.execute());
            print("Inserted docs: " + (i * bulkSize));
        }
        print("inserted documents into hotColl: " + numDocs);

        assert.commandWorked(hotDB.adminCommand({fsync: 1}));
    }

    coldColl.dropIndex({indexField: 1});

    print("reading hotColl into cache");

    // Do a collection scan of the hot collection to fill up the cache.
    let cur = hotColl.find();
    while (cur.hasNext()) {
        cur.next();
    }

    print("Hot cache: ");
    printjson(hotColl.stats().wiredTiger.cache);
    print("Cold cache: ");
    printjson(coldColl.stats().wiredTiger.cache);

    // Start readers to fill up the cache.
    TestData.background = config.background;
    let buildFunc = function() {
        // Build index
        let startTime = Date.now();
        assert.commandWorked(db.getSiblingDB('cold').coldColl.createIndex(
            {indexField: 1}, {background: TestData.background}));
        let endTime = Date.now();
        jsTest.log("Index build took " + (endTime - startTime) + " ms.");
    };

    let shellAwait = startParallelShell(buildFunc, conn.port);
    print("started worker thread");

    const benchInterval = 1;
    let benchArgs = {
        db: hotDbName,
        ops:
            [{ns: hotColl.getFullName(), op: "findOne", query: {_id: {"#RAND_INT": [0, numDocs]}}}],
        parallel: config.numThreads,
        seconds: benchInterval,
        host: conn.host
    };

    let numIndexes = function(coll) {
        let res = coldDB.runCommand({listIndexes: 'coldColl', maxTimeMS: 1});
        if (res.code == ErrorCodes.MaxTimeMSExpired) {
            print("index build not finished");
            return 1;
        }
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

    print("Hot cache: ");
    printjson(hotColl.stats().wiredTiger.cache);
    print("Cold cache: ");
    printjson(coldColl.stats().wiredTiger.cache);

    // Stop updater thread(s).
    shellAwait();
    print("stopped worker thread ");

    print("final collection scan ");
    // Do a collection scan of the hot collection to fill up the cache.
    let startTime = Date.now();
    cur = hotColl.find();
    while (cur.hasNext()) {
        cur.next();
    }
    let endTime = Date.now();
    jsTest.log("Collection scan took " + (endTime - startTime) + " ms.");

    MongoRunner.stopMongod(conn);

})();
