/**
 * Ensures that oplog entries specific to two-phase index builds are not allow when run through
 * applyOps.
 *
 * @tags: [requires_replication]
 */

(function() {

    const coll = db.twoPhaseIndexOps;
    coll.drop();

    const cmdNs = db.getName() + ".$cmd";

    coll.insert({a: 1});

    assert.commandFailedWithCode(db.adminCommand({
        applyOps: [
            {op: "c", ns: cmdNs, o: {startIndexBuild: coll.getName(), key: {a: 1}, name: 'a_1'}}
        ]
    }),
                                 [ErrorCodes.CommandNotSupported, ErrorCodes.FailedToParse]);

    assert.commandFailedWithCode(db.adminCommand({
        applyOps: [{
            op: "c",
            ns: cmdNs,
            o: {commitIndexBuild: coll.getName(), key: {a: 1}, name: 'a_1'}
        }]
    }),
                                 [ErrorCodes.CommandNotSupported, ErrorCodes.FailedToParse]);

    assert.commandFailedWithCode(db.adminCommand({
        applyOps: [
            {op: "c", ns: cmdNs, o: {abortIndexBuild: coll.getName(), key: {a: 1}, name: 'a_1'}}
        ]
    }),
                                 [ErrorCodes.CommandNotSupported, ErrorCodes.FailedToParse]);
})();

