/**
 * Tests basic functionality of two-phase index builds.
 */

(function() {

    load("jstests/libs/fixture_helpers.js");  // for FixtureHelpers

    if (FixtureHelpers.isMongos(db)) {
        print("two-phase index build commands not accepted on mongos");
        return;
    }

    const coll = db.twoPhaseIndexBuild;
    coll.drop();

    const bulk = coll.initializeUnorderedBulkOp();
    const numDocs = 1000;
    for (let i = 0; i < numDocs; i++) {
        bulk.insert({a: i});
    }
    assert.commandWorked(bulk.execute());

    const collName = coll.getName();

    // Create a non-unique index.
    assert.commandWorked(db.runCommand(
        {twoPhaseCreateIndexes: coll.getName(), indexes: [{key: {a: 1}, name: 'a_1'}]}));
    assert.eq(numDocs, coll.find({a: {$gte: 0}}).hint({a: 1}).itcount());
    assert.commandWorked(coll.dropIndexes("a_1"));

    // Ensure both oplog entries were written to the oplog.
    if (FixtureHelpers.isReplSet(db)) {
        const cmdNs = db.getName() + ".$cmd";
        const localDB = db.getSiblingDB("local");
        const oplogColl = localDB.oplog.rs;

        // If this test runs more than once there may be more than one oplog entry.
        assert.gte(oplogColl.find({op: "c", ns: cmdNs, "o.startIndexBuild": collName}).itcount(),
                   1);
        assert.gte(oplogColl.find({op: "c", ns: cmdNs, "o.commitIndexBuild": collName}).itcount(),
                   1);
    }

    /*
    // TODO: SERVER-39079 Uncomment when secondaries handle conflicting in-progress index builds.
    // Create a unique index.
    assert.commandWorked(db.runCommand({
        twoPhaseCreateIndexes: coll.getName(),
        indexes: [{key: {a: 1}, unique: true, name: 'a_1'}]
    }));
    assert.eq(numDocs, coll.find({a: {$gte: 0}}).hint({a: 1}).itcount());
    assert.commandWorked(coll.dropIndexes("a_1"));
    */

    /*
    // TODO: SERVER-39239 Uncomment when primaries write abortIndexBuild oplog entries.
    // Insert a duplicate key so creating a unique index fails.
    coll.insert({a: 0});

    // Creating a duplicate index should fail.
    assert.commandFailedWithCode(db.runCommand({
        twoPhaseCreateIndexes: coll.getName(),
        indexes: [{key: {a: 1}, unique: true, name: 'a_1'}]
    }),
                                 ErrorCodes.DuplicateKey);

    // Using the index should fail.
    assert.throws(() => {
        coll.find({a: {$gte: 0}}).hint({a: 1}).itcount();
    });
    */
})();
