/**
 * Tests that WT_PREPARE_CONFLICT errors are retried correctly on read operations.
 * @tag: [requires_wiredtiger]
 */
(function() {
    "strict";

    let conn = MongoRunner.runMongod();
    let testDB = conn.getDB("test");

    let t = testDB.prepare_conflict;
    t.drop();

    // Test different types of operations: removals, updates, and index operations.
    t.createIndex({x: 1});
    t.createIndex({y: 1}, {partialFilterExpression: {_id: {$gte: 500}}, unique: true});
    let rand = {"#RAND_INT": [0, 1000]};
    let ops = [
        {op: "remove", ns: t.getFullName(), query: {_id: rand}},
        {op: "findOne", ns: t.getFullName(), query: {_id: rand}},
        {
          op: "update",
          ns: t.getFullName(),
          query: {_id: rand},
          update: {$inc: {x: 1}},
          upsert: true
        },
        {op: "findOne", ns: t.getFullName(), query: {x: rand}},
        {
          op: "update",
          ns: t.getFullName(),
          query: {_id: rand},
          update: {$inc: {y: 1}},
          upsert: true
        },
        {op: "findOne", ns: t.getFullName(), query: {y: rand}},
        {op: "findOne", ns: t.getFullName(), query: {_id: rand}},
    ];

    let seconds = 5;
    let parallel = 5;
    let host = testDB.getMongo().host;

    let benchArgs = {ops, seconds, parallel, host};

    assert.commandWorked(testDB.adminCommand(
        {configureFailPoint: 'WTPrepareConflictForReads', mode: 'alwaysOn', data: {chance: 0.05}}));
    res = benchRun(benchArgs);
    printjson({res});

    assert.commandWorked(
        testDB.adminCommand({configureFailPoint: 'WTPrepareConflictForReads', mode: "off"}));
    res = t.validate();
    assert(res.valid, tojson(res));
    MongoRunner.stopMongod(conn);
})();
