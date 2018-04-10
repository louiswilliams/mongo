(function() {
    "use strict";

    load('jstests/replsets/libs/secondary_reads_test.js');

    const name = "secondaryReadsTimestampVisibility";
    const collName = "testColl";
    let srt = new SecondaryReadsTest(name);
    let rst = srt.getReplset();

    let primaryDB = srt.getPrimaryDB();
    let secondaryDB = srt.getSecondaryDB();

    let primaryColl = primaryDB.getCollection(collName);

    // Create a collection and an index. Insert some data.
    primaryDB.runCommand({drop: collName});
    assert.commandWorked(primaryDB.runCommand({create: collName}));
    for (let i = 0; i < 100; i++) {
        assert.commandWorked(primaryColl.insert({_id: i, x: 0}));
    }

    rst.awaitReplication();
    assert.eq(secondaryDB.getCollection(collName).find({x: 0}).itcount(), 100);

    // Prevent a batch from completing on the secondary.
    assert.commandWorked(secondaryDB.adminCommand(
        {configureFailPoint: "pauseMultiApplyBeforeCompletion", mode: "alwaysOn"}));

    // Update x in each document.
    for (let i = 0; i < 100; i++) {
        primaryColl.update({_id: i}, {$set: {x: 1}});
    }
    assert.eq(primaryColl.find({x: 1}).itcount(), 100);

    // Wait for a write concern error because the secondary batch can't complete.
    let res = primaryDB.runCommand(
        {insert: collName, documents: [{_id: 100}], writeConcern: {w: "majority", wtimeout: 3000}});
    assert(res.writeConcernError);

    let levels = ["local", "available", "majority"];
    for (let i in levels) {
        // We should see the previous state on the secondary with every readconcern.
        assert.eq(secondaryDB.getCollection(collName).find({x: 0}).readConcern(levels[i]).itcount(),
                  100);
        assert.eq(secondaryDB.getCollection(collName).find({x: 1}).readConcern(levels[i]).itcount(),
                  0);
    }

    // Disable the failpoint and let the batch complete
    assert.commandWorked(secondaryDB.adminCommand(
        {configureFailPoint: "pauseMultiApplyBeforeCompletion", mode: "off"}));

    rst.awaitReplication();
    srt.stop();
})();
