(function() {
    "use strict";

    let rst = new ReplSetTest({nodes: 2});
    rst.startSet();
    rst.initiate();

    let primary = rst.getPrimary();
    let secondary = rst.getSecondary();
    let testDB = primary.getDB("test");
    let oplogColl = primary.getDB("local").oplog.rs;
    let testCollName = "testColl";
    let testNs = "test." + testCollName;

    testDB.runCommand({drop: testCollName});
    assert.commandWorked(testDB.runCommand({create: testCollName}));
    // Create a unique index on the collection in the foreground.
    assert.commandWorked(testDB.runCommand(
        {createIndexes: testCollName, indexes: [{key: {x: 1}, name: "x_1", unique: true}]}));

    // We want to do applyOps with many different documents, each incrementing a uniquely indexed
    // value, 'x'. The goal is that a reader on a secondary might find a case where the unique
    // index violatation is missed, and an index on x maps to two different records.
    const nOps = 64;
    const nIterations = 100;
    const nReaders = 32;

    // Do a bunch of reads using the 'x' index on the secondary.
    let readCmd = "db.getMongo().setSlaveOk();" + "for (let i = 0; i < " + nIterations +
        "; i++) {" + "assert.commandWorked(" + "db.getSiblingDB('test').runCommand({find: '" +
        testCollName + "', filter: {x: {$gte: 0}}}));" + " print('done')}";
    print("Read cmd: " + readCmd);
    // Do a bunch of reads on all values of x.
    let readers = [];
    for (let i = 0; i < nReaders; i++) {
        readers[i] = startParallelShell(readCmd, secondary.port);
        print("reader " + i + " started");
    }

    // Generate applyOps operations that increment x on each _id backwards to avoid conficts.
    // When these updates get replicated to the secondary, they might get applied out of order
    // in different batches, which can cause unique key violations.
    for (let times = 0; times < nIterations; times++) {
        // Reset the initial documents. Ensure they have been replicated.
        testDB.runCommand({drop: testCollName});
        assert.commandWorked(testDB.runCommand({create: testCollName}));
        for (let i = 0; i < nOps; i++) {
            testDB.runCommand({
                update: testCollName,
                updates: [{q: {_id: i}, u: {$set: {x: i}}}],
                writeConcern: {w: "majority"},
                upsert: true
            });
        }
        // Do updates
        let ops = [];
        for (let i = 0; i < nOps; i++) {
            // Do this nOps+1 times to do a complete cycle of every document to every value of x and
            // back to its orignal value.
            let end = nOps - i - 1;  // start with the nth _id
            let nextX = end + 1;
            ops[i] = {op: "u", ns: testNs, o2: {_id: end}, o: {x: nextX}};
        }
        print('iteration ' + times);
        assert.commandWorked(testDB.runCommand({applyOps: ops}));
    }

    for (let i = 0; i < nReaders; i++) {
        let readerWait = readers[i];
        readerWait();
        print("reader " + i + " done");
    }

    rst.stopSet();
})();
