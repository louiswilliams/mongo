/**
 * This test corrupts WiredTiger data files on a secondary, repairs the data, and asserts that the
 * node is unable to re-join its original replica set without an initial sync.
 *
 * @tags: [requires_wiredtiger]
 */

(function() {

    load('jstests/disk/libs/wt_file_helper.js');

    const baseName = "wt_repair_missing_files";
    const dbName = "repair_missing_files";
    const collName = "test";

    let replSet = new ReplSetTest({nodes: 2});
    replSet.startSet();
    replSet.initiate();
    replSet.awaitReplication();

    const originalSecondary = replSet.getSecondary();

    let primaryDB = replSet.getPrimary().getDB(dbName);
    let secondaryDB = originalSecondary.getDB(dbName);

    assert.writeOK(primaryDB[collName].insert({_id: 0, foo: "bar"}));
    replSet.awaitLastOpCommitted();

    const secondaryPort = originalSecondary.port;
    const secondaryDbpath = originalSecondary.dbpath;

    let secondary = originalSecondary;

    //
    // 1. This repairs the data on a clean data directory and asserts that the node is still able
    // to re-join its original replica set without an initial sync.
    //

    // Shut down the secondary.
    MongoRunner.stopMongod(secondary);

    // Ensure the secondary can be repaired successfully.
    jsTestLog("Repairing the secondary");
    assertRepairSucceeds(secondaryPort, secondaryDbpath);

    // Starting up without --replSet should not fail, and the collection should exist with its data.
    jsTestLog(
        "The repaired secondary should start up and serve reads without the --replSet option");
    secondary =
        MongoRunner.runMongod({dbpath: secondaryDbpath, port: secondaryPort, noCleanData: true});
    assert(secondary);
    secondaryDB = secondary.getDB(dbName);
    assert(secondaryDB[collName].exists());
    assert.eq(secondaryDB[collName].find().itcount(), 1);
    MongoRunner.stopMongod(secondary);

    // Starting the secondary with a wiped data directory should force an initial sync.
    jsTestLog("The secondary with a wiped data directory should resync successfully");
    secondary = replSet.start(originalSecondary,
                              {dbpath: secondaryDbpath, port: secondaryPort, startClean: false});
    secondaryDB = secondary.getDB(dbName);

    // Wait for the secondary to catch up.
    replSet.awaitSecondaryNodes();
    assert.eq(secondaryDB[collName].find().itcount(), 1);

    //
    //
    // 2. This test corrupts WiredTiger data files on a secondary, repairs the data, and asserts
    // that the node is unable to re-join its original replica set without an initial sync.
    //
    //

    let secondaryCollUri = getUriForColl(secondaryDB[collName]);
    let secondaryCollFile = secondaryDbpath + "/" + secondaryCollUri + ".wt";
    // Shut down the secondary. Delete the collection's data file.
    MongoRunner.stopMongod(secondary);
    jsTestLog("Deleting secondary collection file: " + secondaryCollFile);
    removeFile(secondaryCollFile);

    // Ensure the secondary can be repaired successfully.
    jsTestLog("Repairing the secondary");
    assertRepairSucceeds(secondaryPort, secondaryDbpath);

    // Starting up with --replSet should fail with a specific error.
    jsTestLog("The repaired secondary should fail to start up with the --replSet option");
    clearRawMongoProgramOutput();
    secondary = MongoRunner.runMongod({
        dbpath: secondaryDbpath,
        port: secondaryPort,
        replSet: replSet.getReplSetConfig()._id,
        noCleanData: true,
        waitForConnect: false
    });
    assert.soon(function() {
        return rawMongoProgramOutput().indexOf("Fatal Assertion 50895") >= 0;
    });
    MongoRunner.stopMongod(secondary, null, {allowedExitCode: MongoRunner.EXIT_ABRUPT});

    // Starting up without --replSet should not fail, but the collection should exist with no data.
    jsTestLog(
        "The repaired secondary should start up and serve reads without the --replSet option");
    secondary =
        MongoRunner.runMongod({dbpath: secondaryDbpath, port: secondaryPort, noCleanData: true});
    assert(secondary);
    secondaryDB = secondary.getDB(dbName);
    assert(secondaryDB[collName].exists());
    assert.eq(secondaryDB[collName].find().itcount(), 0);
    MongoRunner.stopMongod(secondary);

    // Starting the secondary with a wiped data directory should force an initial sync.
    jsTestLog("The secondary with a wiped data directory should resync successfully");
    secondary = replSet.start(originalSecondary, {dbpath: secondaryDbpath, port: secondaryPort});
    secondaryDB = secondary.getDB(dbName);

    // Wait for the secondary to catch up.
    replSet.awaitSecondaryNodes();
    assert.eq(secondaryDB[collName].find().itcount(), 1);

    //
    //
    // 3. This test corrupts the _mdb_catalog file on a secondary, repairs the data, and asserts
    // that the node is unable to re-join its original replica set without an initial sync.
    //
    //

    // Shut down the secondary. Delete the catalog file.
    MongoRunner.stopMongod(secondary);
    let mdbCatalogFile = secondaryDbpath + "/_mdb_catalog.wt";
    jsTestLog("Deleting secondary catalog file: " + mdbCatalogFile);
    removeFile(mdbCatalogFile);

    // Ensure the secondary can be repaired successfully.
    jsTestLog("Repairing the secondary");
    assertRepairSucceeds(secondaryPort, secondaryDbpath);

    // Starting up with --replSet should fail with a specific error.
    jsTestLog("The repaired secondary should fail to start up with the --replSet option");
    clearRawMongoProgramOutput();
    secondary = MongoRunner.runMongod({
        dbpath: secondaryDbpath,
        port: secondaryPort,
        replSet: replSet.getReplSetConfig()._id,
        noCleanData: true,
        waitForConnect: false
    });
    assert.soon(function() {
        return rawMongoProgramOutput().indexOf("Fatal Assertion 50895") >= 0;
    });
    MongoRunner.stopMongod(secondary, null, {allowedExitCode: MongoRunner.EXIT_ABRUPT});

    // Starting up without --replSet should not fail, but the collection should exist with no data.
    jsTestLog(
        "The repaired secondary should start up and serve reads without the --replSet option");
    secondary =
        MongoRunner.runMongod({dbpath: secondaryDbpath, port: secondaryPort, noCleanData: true});
    assert(secondary);
    secondaryDB = secondary.getDB(dbName);
    assert(secondaryDB[collName].exists());
    assert.eq(secondaryDB[collName].find().itcount(), 0);
    MongoRunner.stopMongod(secondary);

    // Starting the secondary with a wiped data directory should force an initial sync.
    jsTestLog("The secondary with a wiped data directory should resync successfully");
    secondary = replSet.start(originalSecondary, {dbpath: secondaryDbpath, port: secondaryPort});
    secondaryDB = secondary.getDB(dbName);

    // Wait for the secondary to catch up.
    replSet.awaitSecondaryNodes();
    assert.eq(secondaryDB[collName].find().itcount(), 1);

    replSet.stopSet();

})();
