/**
 * Tests that --repair on WiredTiger correctly and gracefully handles corrupt metadata files.
 *
 * @tags: [requires_wiredtiger]
 */

(function() {

    load('jstests/disk/libs/wt_file_helper.js');

    const baseName = "wt_repair_corrupt_files";
    const collName = "test";
    const dbpath = MongoRunner.dataPath + baseName + "/";

    /**
     * Run the test by supplying additional paramters to MongoRunner.runMongod with 'mongodOptions'.
     */
    let runTest = function(mongodOptions) {
        resetDbpath(dbpath);
        jsTestLog("Running test with args: " + tojson(mongodOptions));

        /**
         * Test 1. Create a collection, corrupt the WiredTiger metadata by replacing the .turtle
         * file with an older version. A salvage operation will be run and repair should be
         * successful, either in recovering the collection metadata, or discarding it. There are no
         * guarantees about the existence of the data once salvaged, only that the collection
         * exists.
         */

        const turtleFile = dbpath + "WiredTiger.turtle";
        const turtleFileWithoutCollection = dbpath + "WiredTiger.turtle.1";
        const turtleFileWithCollectionAndData = dbpath + "WiredTiger.turtle.2";

        let mongod = startMongodOnExistingPath(dbpath, mongodOptions);

        jsTestLog("Copying metadata file before creating the collection: " +
                  turtleFileWithoutCollection);
        copyFile(turtleFile, turtleFileWithoutCollection);

        let testColl = mongod.getDB(baseName)[collName];
        assert.commandWorked(testColl.insert({a: 1}));

        // Force a checkpoint with fsync because journaling is disabled.
        assert.commandWorked(mongod.getDB(baseName).adminCommand({fsync: 1}));

        jsTestLog("Copying metadata file after creating the collection and inserting data: " +
                  turtleFileWithCollectionAndData);
        copyFile(turtleFile, turtleFileWithCollectionAndData);

        assert.commandWorked(testColl.insert({a: 2}));

        // A clean shutdown will write a different checkpoint.
        MongoRunner.stopMongod(mongod);

        // Guarantee the turtle files changed after each checkpoint.
        assert.neq(md5sumFile(turtleFileWithoutCollection),
                   md5sumFile(turtleFileWithCollectionAndData));
        assert.neq(md5sumFile(turtleFileWithoutCollection), md5sumFile(turtleFile));

        jsTestLog("Replacing metadata file with an version after the collection and data existed.");
        removeFile(turtleFile);
        copyFile(turtleFileWithCollectionAndData, turtleFile);

        assertRepairSucceeds(dbpath, mongod.port, mongodOptions);

        mongod = startMongodOnExistingPath(dbpath, mongodOptions);
        testColl = mongod.getDB(baseName)[collName];

        assert(testColl.exists());
        // Don't assert about the existence, because salvage makes no guarantees.
        print("Found documents in collection: " + testColl.find({}).itcount());

        MongoRunner.stopMongod(mongod);

        // TODO: THIS DOESN'T WORK YET.
        /*
        jsTestLog("Replacing metadata file with an version before the collection existed.");
        removeFile(turtleFile);
        copyFile(turtleFileWithoutCollection, turtleFile);

        assertRepairSucceeds(dbpath, mongod.port, mongodOptions);

        mongod = startMongodOnExistingPath(dbpath, mongodOptions);
        testColl = mongod.getDB(baseName)[collName];

        assert(testColl.exists());
        print("Found documents in collection: " + testColl.find({}).itcount());
        MongoRunner.stopMongod(mongod);
        */
    };

    // Run without the journal so that fsync forces a checkpoint.
    runTest({nojournal: ""});
})();
