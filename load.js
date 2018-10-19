const nDocs = 5 * 1000 * 1000;

Random.setRandomSeed(new Date());

let padding = 'x'.repeat(1000);

db.bulkInsertTest.drop();
for (let i = 0; i < nDocs / 1000; i++) {
    let bulk = db.bulkInsertTest.initializeUnorderedBulkOp();
    for (let j = 0; j < 1000; j++) {
        let rand = Random.randInt(nDocs);
        let seq = (i * 1000) + j;
        bulk.insert({i: seq, rand: rand, x: padding});
    }
    assert.commandWorked(bulk.execute());
    print("i: " + i * 1000);
}

db.bulkTest.drop();
for (let i = 0; i < nDocs / 1000; i++) {
    let bulk = db.bulkTest.initializeUnorderedBulkOp();
    for (let j = 0; j < 1000; j++) {
        let rand = Random.randInt(nDocs);
        let seq = (i * 1000) + j;
        bulk.insert({i: seq, rand: rand, x: padding});
    }
    assert.commandWorked(bulk.execute());
    print("i: " + i * 1000);
}
