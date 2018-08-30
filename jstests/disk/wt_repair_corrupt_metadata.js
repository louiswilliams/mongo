/**
 * Tests that --repair on WiredTiger correctly and gracefully handles corrupt metadata files.
 *
 * @tags: [requires_wiredtiger]
 */

(function() {

    load('jstests/disk/libs/wt_file_helper.js');

    const baseName = "wt_repair_corrupt_metadata";
    const collName = "test";
    const dbpath = MongoRunner.dataPath + baseName + "/";

    /**
     * Run the test by supplying additional parameters to MongoRunner.runMongod with
     * 'mongodOptions'.
     */
    let runTest = function(mongodOptions) {
        resetDbpath(dbpath);
        jsTestLog("Running test with args: " + tojson(mongodOptions));

        /**
         * Create a collection and corrupt the WiredTiger metadata by replacing the
         * WiredTiger.turtle file with older versions. This file contains checkpoint information
         * about the WiredTiger.wt file, so if these two files get out of sync, WiredTiger will have
         * to attempt a salvage operation on the .wt file and rebuild the .turtle file. This could
         * involve recovering the collection metadata or discarding, depending on the extent of the
         * damage.
         *
         * These are two different scenarios to consider:
         *
         * 1. Run repair using a version of the turtle file that has checkpoint information after
         * the collection was created. The expectation is that the metadata salvage on the turtle
         * file should be successful, and the data in the collection should still be still visible.
         *
         * 2. Run repair using a version of the turtle file that has checkpoint information before
         * the collection was created. The expectation is that the metadata salvage will not be
         * successful (but not fail), and the collection will be recreated without any data.
         */

        const turtleFile = dbpath + "WiredTiger.turtle";
        const turtleFileWithoutCollection = dbpath + "WiredTiger.turtle.1";
        const turtleFileWithCollectionAndData = dbpath + "WiredTiger.turtle.2";

        let mongod = startMongodOnExistingPath(dbpath, mongodOptions);

        jsTestLog("Making copy of metadata file before creating the collection: " +
                  turtleFileWithoutCollection);
        copyFile(turtleFile, turtleFileWithoutCollection);

        let testColl = mongod.getDB(baseName)[collName];
        assert.commandWorked(testColl.insert({a: 1}));

        // Force a checkpoint with fsync because journaling is disabled.
        assert.commandWorked(mongod.getDB(baseName).adminCommand({fsync: 1}));

        jsTestLog("Making copy of metadta file after creating the collection and inserting data: " +
                  turtleFileWithCollectionAndData);
        copyFile(turtleFile, turtleFileWithCollectionAndData);

        assert.commandWorked(testColl.insert({a: 2}));

        // A clean shutdown will write a different checkpoint.
        MongoRunner.stopMongod(mongod);

        // Guarantee the turtle files changed between each checkpoint.
        assert.neq(md5sumFile(turtleFileWithoutCollection),
                   md5sumFile(turtleFileWithCollectionAndData));
        assert.neq(md5sumFile(turtleFileWithCollectionAndData), md5sumFile(turtleFile));

        jsTestLog("Replacing metadata file with a version where the collection existed.");
        removeFile(turtleFile);
        copyFile(turtleFileWithCollectionAndData, turtleFile);

        assertRepairSucceeds(dbpath, mongod.port, mongodOptions);

        mongod = startMongodOnExistingPath(dbpath, mongodOptions);
        testColl = mongod.getDB(baseName)[collName];

        assert(testColl.exists());
        // We can assert that the data exists because the salvage only took place on the metadata,
        // not the data.
        assert.eq(testColl.find({}).itcount(), 2);

        MongoRunner.stopMongod(mongod);

        // Unfortunately, WiredTiger aborts and dumps core when WT_PANIC is returned on debug
        // builds. The case below triggers a WT_PANIC, which means that normal error handling in
        // MongoDB is impossible.
        if (!db.adminCommand('buildInfo').debug) {
            jsTestLog("Replacing metadata file with an version before the collection existed.");
            removeFile(turtleFile);
            copyFile(turtleFileWithoutCollection, turtleFile);

            assertRepairSucceeds(dbpath, mongod.port, mongodOptions);

            mongod = startMongodOnExistingPath(dbpath, mongodOptions);
            testColl = mongod.getDB(baseName)[collName];

            // Assert the existence of the collection only because it was recovered.
            assert(testColl.exists());
            assert.eq(testColl.find({}).itcount(), 2);
            MongoRunner.stopMongod(mongod);
        } else {
            jsTestLog("Skipping test case because running on a debug build.");
        }
    };

    // Run without the journal so that fsync forces a checkpoint.
    runTest({nojournal: ""});
})();
