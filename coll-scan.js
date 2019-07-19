(function() {
    "use strict";

    const collA = db.A;
    const collB = db.B;
    collA.drop();
    collB.drop();

    assert.commandWorked(db.runCommand({create: collA.getName(), clusteredIdIndex: false}));
    assert.commandWorked(db.runCommand({create: collB.getName(), clusteredIdIndex: true}));

    const targetDataSize = 100 * 1024 * 1024;
    const docSize = 100;

    const nDocs = targetDataSize / docSize;
    print("Inserting " + nDocs + " documents");

    Random.setRandomSeed(new Date());
    [collA, collB].forEach(function(coll) {
        const start = new Date();

        const bulk = coll.initializeUnorderedBulkOp();
        for (let i = 1; i <= nDocs; i++) {
            bulk.insert({
                // Space out range to reduce possibility of duplicates
                _id: NumberInt(Random.randInt(nDocs * 100)),
                a: 'x'.repeat(docSize),
            });
        }
        try {
            bulk.execute();
        } catch (e) {
            print("Ignored write errors");
        }

        const end = new Date();
        const elapsed = end - start;
        print('inserted ' + nDocs + ' docs into ' + coll.getName() + ' in ' + elapsed +
              'ms. ops/s: ' + (nDocs / (elapsed / 1000)));

        const lookupRes = benchRun({
            ops: [
                {op: "find", ns: coll.getFullName(), query: {_id: {"#RAND_INT": [1, nDocs]}}},
            ],
            seconds: 10,
            host: db.getMongo().host
        });
        printjson(lookupRes);

        const scanRes = benchRun({
            ops: [
                {
                  op: "find",
                  ns: coll.getFullName(),
                  query: {_id: {$gte: {"#RAND_INT": [1, nDocs]}}},
                  limit: 10000
                },
            ],
            seconds: 30,
            host: db.getMongo().host
        });
        printjson(scanRes);
    });
}());

