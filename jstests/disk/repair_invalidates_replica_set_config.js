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
    assertRepairSucceeds(secondaryPort, secondaryDbpath);

    // Starting up without --replSet should not fail, and the collection should exist with its data.
    assertStartAndStopStandaloneOnExistingDbpath(secondaryDbpath, secondaryPort, function(node) {
        let nodeDB = node.getDB(dbName);
        assert(nodeDB[collName].exists());
        assert.eq(nodeDB[collName].find().itcount(), 1);
    });

    // Starting the secondary with a wiped data directory should force an initial sync.
    secondary = assertStartAndResync(replSet, originalSecondary, function(node) {
        let nodeDB = node.getDB(dbName);
        assert.eq(nodeDB[collName].find().itcount(), 1);
    });
    secondaryDB = secondary.getDB(dbName);

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
    assertRepairSucceeds(secondaryPort, secondaryDbpath);

    // Starting up with --replSet should fail with a specific error.
    assertErrorOnStartupWhenStartingAsReplSet(
        secondaryDbpath, secondaryPort, replSet.getReplSetConfig()._id);

    // Starting up without --replSet should not fail, but the collection should exist with no data.
    assertStartAndStopStandaloneOnExistingDbpath(secondaryDbpath, secondaryPort, function(node) {
        let nodeDB = node.getDB(dbName);
        assert(nodeDB[collName].exists());
        assert.eq(nodeDB[collName].find().itcount(), 0);
    });

    // Starting the secondary with a wiped data directory should force an initial sync.
    secondary = assertStartAndResync(replSet, originalSecondary, function(node) {
        let nodeDB = node.getDB(dbName);
        assert.eq(nodeDB[collName].find().itcount(), 1);
    });
    secondaryDB = secondary.getDB(dbName);

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
    assertRepairSucceeds(secondaryPort, secondaryDbpath);

    // Starting up with --replSet should fail with a specific error.
    assertErrorOnStartupWhenStartingAsReplSet(
        secondaryDbpath, secondaryPort, replSet.getReplSetConfig()._id);

    // Starting up without --replSet should not fail, but the collection should exist with no data.
    assertStartAndStopStandaloneOnExistingDbpath(secondaryDbpath, secondaryPort, function(node) {
        let nodeDB = node.getDB(dbName);
        assert(!nodeDB[collName].exists());
    });

    // Starting the secondary with a wiped data directory should force an initial sync.
    secondary = assertStartAndResync(replSet, originalSecondary, function(node) {
        let nodeDB = node.getDB(dbName);
        assert.eq(nodeDB[collName].find().itcount(), 1);
    });

    replSet.stopSet();

})();
