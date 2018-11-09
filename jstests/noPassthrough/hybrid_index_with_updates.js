

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

    for (let i = 0; i < 10; i++) {
        assert.commandWorked(testDB.hybrid.insert({i: i}));
    }

    // Hang the build after the first document.
    let stopKey = 1;
    turnFailPointOn("hangBeforeIndexBuildOf", stopKey);

    // Start the background build.
    let bgBuild = startParallelShell(function() {
        assert.commandWorked(db.hybrid.createIndex({i: 1}, {background: true}));
    }, conn.port);

    checkLog.contains(conn, "Hanging before index build of i=" + stopKey);

    // Do some updates, inserts and deletes while building.
    for (let i = 0; i < 5; i++) {
        assert.commandWorked(testDB.hybrid.update({i: i}, {i: -1}));
    }
    for (let i = 5; i < 10; i++) {
        assert.commandWorked(testDB.hybrid.remove({i: i}));
    }
    for (let i = 10; i < 15; i++) {
        assert.commandWorked(testDB.hybrid.insert({i: i}));
    }

    // Enable pause after bulk dump into index and first drain.
    turnFailPointOn("hangAfterDumpInsertsFromBulk");
    // Allow first drain to finish.
    turnFailPointOff("hangBeforeIndexBuildOf");

    // Wait for first drain to complete.
    checkLog.contains(conn, "Hanging after dumping inserts from bulk builder");

    // Add inserts that must be consumed in the second drain.
    for (let i = 15; i < 25; i++) {
        assert.commandWorked(testDB.hybrid.insert({i: i}));
    }

    // Enable pause after final drain.
    turnFailPointOn("hangAfterIndexBuildReleasesSharedLock");

    // Allow second drain to complete.
    turnFailPointOff("hangAfterDumpInsertsFromBulk");

    // Wait for second drain to finish.
    checkLog.contains(conn, "Hanging after releasing shared lock");

    // Add inserts that must be consumed in the final drain.
    for (let i = 25; i < 30; i++) {
        assert.commandWorked(testDB.hybrid.insert({i: i}));
    }

    // Wait for final drain.
    turnFailPointOff("hangAfterIndexBuildReleasesSharedLock");

    bgBuild();

    assert.eq(25, testDB.hybrid.count());
    assert.commandWorked(testDB.hybrid.validate());

    MongoRunner.stopMongod(conn);
})();

