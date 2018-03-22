/**
 * This test ensures readers on a secondary are unable to read from unique indexes that temporarily
 * violate their uniqueness constraint. When oplog entries are applied in parallel batch writes,
 * operations can be applied out-of-order, and reading at an inconsistent state is possible.
 *
 * For example, take two documents, { _id: 1, x: 1} and  { _id: 2, x: 2} with a unique index on 'x'.
 * On the primary, two updates take place in this order:
 *   { _id: 2, x: 3 }
 *   { _id: 1, x: 2 }
 * There is no uniqueness violation here because x: 3 happens before x: 2. If the updates had
 * occured in the opposite order, a DuplicateKey error would be returned to the user.
 *
 * When these operations are applied on a secondary, they split up across one of 16 writer threads,
 * hashed on the _id of the document. This guarantees that updates to the same document will occur
 * in-order on the same thread.
 *
 * Take two parallel threads applying the same updates as before:
 *      Thread 1            Thread 2
 *   { _id: 1, x: 2 }
 *                       { _id: 2, x: 3 }
 *
 * If a secondary reader were to access the index entry for x=2 after Thread 1 made its update but
 * before Thread 2 made its update, they would find two entries for x=2, which is a violation of the
 * uniqueness constraint. When applying operations in parallel like this, we temporarily ignore
 * uniqueness violations on indexes, and require readers on secondaries to wait for the parallel
 * batch insert to complete, at which point the state of the indexes will be consistent.
 */
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

    // We want to do applyOps with at last as many different documents as there are parallel batch
    // writer threads (16). Each iteration increments and decrements a uniquely indexed value, 'x'.
    // The goal is that a reader on a secondary might find a case where the unique index constraint
    // is ignored, and an index on x maps to two different records.
    const nOps = 16;
    const nIterations = 50;
    const nReaders = 16;

    // Do a bunch of reads using the 'x' index on the secondary.
    let readCmd = "db.getMongo().setSlaveOk();" + "for (let i = 0; i < " + nIterations +
        "; i++) { for (let x = 0; x < " + nOps + "; x++) {" + "assert.commandWorked(" +
        "db.getSiblingDB('test').runCommand({find: '" + testCollName +
        "', filter: {x: x}, projection: {x: 1}}));}}";
    print("Read cmd: " + readCmd);
    let readers = [];
    for (let i = 0; i < nReaders; i++) {
        readers[i] = startParallelShell(readCmd, secondary.port);
        print("reader " + i + " started");
    }

    // Write the initial documents. Ensure they have been replicated.
    for (let i = 0; i < nOps; i++) {
        assert.commandWorked(testDB.runCommand({
            insert: testCollName,
            documents: [{_id: i, x: i, iter: 0}],
            writeConcern: {w: "majority"}
        }));
    }

    // Cycle the value of x in the document {_id: i, x: i} between i and i+1 each iteration.
    for (let iteration = 0; iteration < nIterations; iteration++) {
        let ops = [];
        // Reset each document.
        for (let i = 0; i < nOps; i++) {
            ops[i] = {op: "u", ns: testNs, o2: {_id: i}, o: {x: i, iter: iteration}};
        }
        assert.commandWorked(testDB.runCommand({applyOps: ops}));
        ops = [];

        // Generate an applyOps command that increments x on each document backwards by _id to avoid
        // conficts when applied in-order. When these updates get applied to the secondary, they may
        // get applied out of order by different threads and temporarily violate unique index
        // constraints.
        for (let i = 0; i < nOps; i++) {
            // Start at the end and increment x by 1.
            let end = nOps - i - 1;
            let nextX = end + 1;
            ops[i] = {op: "u", ns: testNs, o2: {_id: end}, o: {x: nextX, iter: iteration}};
        }
        print('iteration ' + iteration);
        assert.commandWorked(testDB.runCommand({applyOps: ops}));
    }

    for (let i = 0; i < nReaders; i++) {
        let readerWait = readers[i];
        readerWait();
        print("reader " + i + " done");
    }
    rst.stopSet();
})();
