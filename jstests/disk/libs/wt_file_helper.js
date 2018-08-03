/**
 * Get the URI of the wt collection file given the collection name.
 */
let getUriForColl = function(coll) {
    assert(coll.exists());  // Collection must exist
    return coll.stats().wiredTiger.uri.split("table:")[1];
};

/**
 * Get the URI of the wt index file given the collection name and the index name.
 */
let getUriForIndex = function(coll, indexName) {
    assert(coll.exists());  // Collection must exist
    const ret = assert.commandWorked(coll.getDB().runCommand({collStats: coll.getName()}));
    return ret.indexDetails[indexName].uri.split("table:")[1];
};

/**
 * 'Corrupt' the file by replacing it with an empty file.
 */
let corruptFile = function(file) {
    removeFile(file);
    writeFile(file, "");
};

/**
 * Assert that running MongoDB with --repair on the provided dbpath exits cleanly.
 */
let assertRepairSucceeds = function(port, dbpath) {
    assert.eq(0, runMongoProgram("mongod", "--repair", "--port", port, "--dbpath", dbpath));
};

/**
 * Assert that starting MongoDB with --replSet on an existing data path exits with a specific
 * error.
 */
let assertErrorOnStartupWhenStartingAsReplSet = function(dbpath, port, rsName) {
    clearRawMongoProgramOutput();

    let node = MongoRunner.runMongod(
        {dbpath: dbpath, port: port, replSet: rsName, noCleanData: true, waitForConnect: false});
    assert.soon(function() {
        return rawMongoProgramOutput().indexOf("Fatal Assertion 50895") >= 0;
    });
    MongoRunner.stopMongod(node, null, {allowedExitCode: MongoRunner.EXIT_ABRUPT});
};

let assertStartStandaloneOnExistingDbpath = function(dbpath, port, testFunc) {
    let node = MongoRunner.runMongod({dbpath: dbpath, port: port, noCleanData: true});
    assert(node);
    testFunc(node);
    MongoRunner.stopMongod(node);
};

/**
 * Assert certain error messages are thrown on startup when files are missing or corrupt.
 */
let assertErrorOnStartupWhenFilesAreCorruptOrMissing = function(
    dbpath, dbName, collName, deleteOrCorruptFunc, errmsg) {
    // Start a MongoDB instance, create the collection file.
    const mongod = MongoRunner.runMongod({dbpath: dbpath, cleanData: true});
    const testColl = mongod.getDB(dbName)[collName];
    const doc = {a: 1};
    assert.writeOK(testColl.insert(doc));

    // Stop MongoDB and corrupt/delete certain files.
    deleteOrCorruptFunc(mongod, testColl);

    // Restart the MongoDB instance and get an expected error message.
    clearRawMongoProgramOutput();
    assert.eq(MongoRunner.EXIT_ABRUPT,
              runMongoProgram("mongod", "--port", mongod.port, "--dbpath", dbpath));
    assert.gte(rawMongoProgramOutput().indexOf(errmsg), 0);
};

/**
 * Assert certain error messages are thrown on a specific request when files are missing or corrupt.
 */
let assertErrorOnRequestWhenFilesAreCorruptOrMissing = function(
    dbpath, dbName, collName, deleteOrCorruptFunc, requestFunc, errmsg) {
    // Start a MongoDB instance, create the collection file.
    mongod = MongoRunner.runMongod({dbpath: dbpath, cleanData: true});
    testColl = mongod.getDB(dbName)[collName];
    const doc = {a: 1};
    assert.writeOK(testColl.insert(doc));

    // Stop MongoDB and corrupt/delete certain files.
    deleteOrCorruptFunc(mongod, testColl);

    // Restart the MongoDB instance.
    clearRawMongoProgramOutput();
    mongod = MongoRunner.runMongod({dbpath: dbpath, port: mongod.port, noCleanData: true});

    // This request crashes the server.
    testColl = mongod.getDB(dbName)[collName];
    requestFunc(testColl);

    // Get an expected error message.
    assert.gte(rawMongoProgramOutput().indexOf(errmsg), 0);
    MongoRunner.stopMongod(mongod, 9, {allowedExitCode: MongoRunner.EXIT_ABRUPT});
};
