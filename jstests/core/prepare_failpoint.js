(function() {
    "use strict";
    // For isReplSet
    load("jstests/libs/fixture_helpers.js");

    let t = db.prepare_txn;

    if (!FixtureHelpers.isReplSet(db)) {
        jsTestLog("Skipping test as doTxn requires a replSet and replication is not enabled.");
        return;
    }

    var session = db.getMongo().startSession();
    db = session.getDatabase("test");
    var txnNumber = 0;

    assert.commandWorked(db.createCollection(t.getName()));

    /**
     * This failpoint does the following instead of the default commit behavior:
     * - Skips running the onTransactionCommit OpObserver which writes the applyOps oplog entry for
     *   the transaction.
     * - Runs the onTransactionPrepare OpObserver to generate an OpTime for the prepare timestamp.
     * - Prepares the transaction in the WiredTiger API with the prepare timestamp.
     * - Does not commit the transaction, and instead lets it abort.
     *
     * As a result, the command will fail with the FailPointEnabled code, and the insert will not
     * complete.
     */
    assert.commandWorked(
        db.adminCommand({configureFailPoint: 'usePrepareTransactionOnCommit', mode: 'alwaysOn'}));

    // Test a single insert.
    var a = assert.commandFailedWithCode(db.adminCommand({
        doTxn: [{"op": "i", "ns": t.getFullName(), "o": {_id: 5, x: 17}}],
        txnNumber: NumberLong(txnNumber++)
    }),
                                         ErrorCodes.FailPointEnabled);
    assert.eq(0, t.find().count(), "Prepare insert succeeded when it should not have.");

    // Insert a document.
    var doc = {_id: 5, x: 17};
    assert.commandWorked(t.insert(doc));

    // Test multiple ops.
    var res = assert.commandFailedWithCode(db.runCommand({
        doTxn: [
            {op: "u", ns: t.getFullName(), o2: {_id: 5}, o: {$set: {x: 18}}},
            {op: "u", ns: t.getFullName(), o2: {_id: 5}, o: {$set: {x: 19}}}
        ],
        txnNumber: NumberLong(txnNumber++)
    }),
                                           ErrorCodes.FailPointEnabled);
    assert.eq(doc, t.findOne(), "Prepare insert succeeded when it should not have.");

    // Test precondition.
    res = assert.commandFailedWithCode(db.runCommand({
        doTxn: [
            {op: "u", ns: t.getFullName(), o2: {_id: 5}, o: {$set: {x: 20}}},
            {op: "u", ns: t.getFullName(), o2: {_id: 5}, o: {$set: {x: 21}}}
        ],
        preCondition: [{ns: t.getFullName(), q: {_id: 5}, res: {x: 17}}],
        txnNumber: NumberLong(txnNumber++)
    }),
                                       ErrorCodes.FailPointEnabled);
    assert.eq(doc, t.findOne(), "Prepare insert succeeded when it should not have.");

}());
