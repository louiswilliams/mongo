(function() {
    "use strict";

    const collA = db.A;
    const collB = db.B;
    collA.drop();
    collB.drop();

    assert.commandWorked(db.runCommand({create: collA.getName(), clusteredIdIndex: false}));
    assert.commandWorked(db.runCommand({create: collB.getName(), clusteredIdIndex: true}));
    const nDocs = 100 * 1000;

    [collA, collB].forEach(function(coll) {
        const start = new Date();
        const bulk = coll.initializeUnorderedBulkOp();
        for (let i = 1; i <= nDocs; i++) {
            bulk.insert({
                _id: i,
                a: 'x'.repeat(10),
            });
        }
        bulk.execute();

        const end = new Date();
        const elapsed = end - start;
        print('inserted ' + nDocs + ' docs into ' + coll.getName() + ' in ' + elapsed +
              'ms. ops/s: ' + (nDocs / elapsed));

        const collRes = benchRun({
            ops: [
                {op: "find", ns: coll.getFullName(), query: {_id: {"#RAND_INT": [1, nDocs]}}},
            ],
            seconds: 10,
            host: db.getMongo().host
        });
        printjson(collRes);
    });

}());

