const nClients = 20;
let shells = [];

let start = new Date();

for (let i = 0; i < nClients; i++) {
    shells.push(startParallelShell(function() {
        let s = new Date();
        Random.setRandomSeed(s);
        let coll = db.bulkInsertTest;
        let count = coll.find({rand: Random.randInt(5 * 1000 * 1000)}).count();
        let e = new Date();
        jsTest.log("Client took " + (e - s) + " seconds. Count: " + count);

    }));
}

for (let i = 0; i < nClients; i++) {
    shells[i]();
}

let end = new Date();

jsTest.log("Test took " + (end - start) + " seconds");
