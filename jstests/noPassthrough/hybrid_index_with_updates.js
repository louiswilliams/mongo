/**
 * Tests that write operations are accepted and result in correct indexing behavior for each phase
 * of hybrid index builds.
 *
 * @tags: [requires_document_locking]
 */
(function() {
    "use strict";

    load("jstests/libs/check_log.js");

    let conn = MongoRunner.runMongod();
    let testDB = conn.getDB('test');

    let turnFailPointOn = function(failPointName, i) {
        assert.commandWorked(testDB.adminCommand(
            {configureFailPoint: failPointName, mode: "alwaysOn", data: {"i": i}}));
    };

    let turnFailPointOff = function(failPointName) {
        assert.commandWorked(testDB.adminCommand({configureFailPoint: failPointName, mode: "off"}));
    };

    let bulk = testDB.hybrid.initializeUnorderedBulkOp();
    for (let i = 0; i < 1000; i++) {
        bulk.insert({i: i});
    }
    assert.commandWorked(bulk.execute());

    // Hang the build after the first document.
    let stopKey = 1;
    turnFailPointOn("hangBeforeIndexBuildOf", stopKey);

    // Start the background build.
    let bgBuild = startParallelShell(function() {
        assert.commandWorked(db.hybrid.createIndex({i: 1}, {background: true}));
    }, conn.port);

    checkLog.contains(conn, "Hanging before index build of i=" + stopKey);

    // Do some updates, inserts and deletes while building.
    bulk = testDB.hybrid.initializeUnorderedBulkOp();
    let i = 0;
    for (; i < 50; i++) {
        bulk.find({i: 1}).update({$set: {i: -i}});
    }
    for (; i < 100; i++) {
        bulk.find({i: i}).remove();
    }
    for (; i < 1500; i++) {
        bulk.insert({i: i});
    }
    assert.commandWorked(bulk.execute());

    // Enable pause after bulk dump into index and first drain.
    turnFailPointOn("hangAfterIndexBuildDumpsInsertsFromBulk");
    // Allow first drain to finish.
    turnFailPointOff("hangBeforeIndexBuildOf");

    // Wait for first drain to complete.
    checkLog.contains(conn, "Hanging after dumping inserts from bulk builder");

    // Add inserts that must be consumed in the second drain.
    bulk = testDB.hybrid.initializeUnorderedBulkOp();
    for (; i < 2000; i++) {
        bulk.insert({i: i});
    }
    assert.commandWorked(bulk.execute());

    // Enable pause after final drain.
    turnFailPointOn("hangAfterIndexBuildReleasesSharedLock");

    // Allow second drain to complete.
    turnFailPointOff("hangAfterIndexBuildDumpsInsertsFromBulk");

    // Wait for second drain to finish.
    checkLog.contains(conn, "Hanging after releasing shared lock");

    // Add inserts that must be consumed in the final drain.
    bulk = testDB.hybrid.initializeUnorderedBulkOp();
    for (; i < 3000; i++) {
        bulk.insert({i: i});
    }
    assert.commandWorked(bulk.execute());

    // Wait for final drain.
    turnFailPointOff("hangAfterIndexBuildReleasesSharedLock");

    // Wait for build to complete.
    bgBuild();

    assert.eq(3850, testDB.hybrid.count());
    assert.commandWorked(testDB.hybrid.validate());

    MongoRunner.stopMongod(conn);
})();
