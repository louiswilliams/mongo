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
    let i = 0;
    for (; i < 1001; i++) {
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

    // Phase 1: Collection scan and external sort
    // Insert documents while doing the bulk build.
    bulk = testDB.hybrid.initializeUnorderedBulkOp();
    for (; i < 2002; i++) {
        bulk.insert({i: i});
    }
    assert.commandWorked(bulk.execute());

    // Enable pause after bulk dump into index.
    turnFailPointOn("hangAfterIndexBuildDumpsInsertsFromBulk");

    // Wait for the bulk insert to complete.
    turnFailPointOff("hangBeforeIndexBuildOf");
    checkLog.contains(conn, "Hanging after dumping inserts from bulk builder");

    // Phase 2: First drain
    // Do some updates, inserts and deletes after the bulk builder has finished.

    // Enable pause after first drain.
    turnFailPointOn("hangAfterIndexBuildFirstDrain");

    bulk = testDB.hybrid.initializeUnorderedBulkOp();
    for (; i < 2102; i++) {
        bulk.find({i: 1}).update({$set: {i: -i}});
    }
    for (; i < 2203; i++) {
        bulk.find({i: i}).remove();
    }
    for (; i < 3004; i++) {
        bulk.insert({i: i});
    }
    assert.commandWorked(bulk.execute());

    // Allow first drain to start.
    turnFailPointOff("hangAfterIndexBuildDumpsInsertsFromBulk");

    // Wait for first drain to finish.
    checkLog.contains(conn, "Hanging after index build first drain");

    // Phase 3: Second drain
    // Enable pause after second drain.
    turnFailPointOn("hangAfterIndexBuildSecondDrain");

    // Add inserts that must be consumed in the second drain.
    bulk = testDB.hybrid.initializeUnorderedBulkOp();
    for (; i < 4004; i++) {
        bulk.insert({i: i});
    }
    assert.commandWorked(bulk.execute());

    // Allow second drain to start.
    turnFailPointOff("hangAfterIndexBuildFirstDrain");

    // Wait for second drain to finish.
    checkLog.contains(conn, "Hanging after index build second drain");

    // Phase 4: Final drain and commit.
    // Add inserts that must be consumed in the final drain.
    bulk = testDB.hybrid.initializeUnorderedBulkOp();
    for (; i < 5005; i++) {
        bulk.insert({i: i});
    }
    assert.commandWorked(bulk.execute());

    // Allow final drain to start.
    turnFailPointOff("hangAfterIndexBuildSecondDrain");

    // Wait for build to complete.
    bgBuild();

    assert.eq(4804, testDB.hybrid.count());
    assert.commandWorked(testDB.hybrid.validate({full: true}));

    MongoRunner.stopMongod(conn);
})();
