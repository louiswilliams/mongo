'use strict';

/**
 * agg_base.js
 *
 * Base workload for aggregation. Inserts a bunch of documents in its setup,
 * then each thread does an aggregation with an empty $match.
 */
var $config = (function() {
    var data = {
        numDocs: 1000,
        // Use 12KB documents by default. This number is useful because 12,000 documents each of
        // size 12KB take up more than 100MB in total, and 100MB is the in-memory limit for $sort
        // and $group.
        docSize: 16 * 1000
    };

    var getStringOfLength = (function() {
        var cache = {};
        return function getStringOfLength(size) {
            if (!cache[size]) {
                cache[size] = new Array(size + 1).join('x');
            }
            return cache[size];
        };
    })();

    function padDoc(doc, size) {
        // first set doc.padding so that Object.bsonsize will include the field name and other
        // overhead
        doc.padding = "";
        var paddingLength = size - Object.bsonsize(doc);
        assertAlways.lte(
            0, paddingLength, 'document is already bigger than ' + size + ' bytes: ' + tojson(doc));
        doc.padding = getStringOfLength(paddingLength);
        assertAlways.eq(size, Object.bsonsize(doc));
        return doc;
    }

    var states = {
        init: function init(db, collName) {
            this.threadCollName = db[collName].getName() + "_" + this.tid;
            assertWhenOwnColl.commandWorked(db.runCommand({create: this.threadCollName}));
        },

        insert: function insert(db, collname) {
            var bulk = db[this.threadCollName].initializeUnorderedBulkOp();
            for (var i = 0; i < this.numDocs; ++i) {
                // note: padDoc caches the large string after allocating it once, so it's ok to call
                // it in this loop
                bulk.insert(padDoc({
                    _id: i,
                    flag: i % 2 ? true : false,
                    rand: Random.rand(),
                    randInt: Random.randInt(this.numDocs)
                },
                                   this.docSize));
            }
            var res = bulk.execute();
            assertWhenOwnColl.writeOK(res);
            assertWhenOwnColl.eq(this.numDocs, res.nInserted);
            assertWhenOwnColl.eq(this.numDocs, db[this.threadCollName].find().itcount());
            assertWhenOwnColl.eq(this.numDocs / 2,
                                 db[this.threadCollName].find({flag: false}).itcount());
            assertWhenOwnColl.eq(this.numDocs / 2,
                                 db[this.threadCollName].find({flag: true}).itcount());
        },
        remove: function remove(db, collName) {
            var bulk = db[this.threadCollName].initializeUnorderedBulkOp();
            bulk.find({}).remove();
            var res = bulk.execute();

            assertWhenOwnColl.writeOK(res);
            assertWhenOwnColl.eq(this.numDocs, res.nRemoved);
            assertWhenOwnColl.eq(0, db[this.threadCollName].find().itcount());
        }
    };

    var transitions = {
        init: {insert: 1},
        insert: {remove: 1},
        remove: {insert: 1},
    };

    function setup(db, collName, cluster) {
    }

    function teardown(db, collName, cluster) {
        // By default, do nothing on teardown. Workload tests may implement this function.
    }

    return {
        // Using few threads and iterations because each iteration is fairly expensive compared to
        // other workloads' iterations. (Each does a collection scan over a few thousand documents
        // rather than a few dozen documents.)
        threadCount: 10,
        iterations: 20,
        states: states,
        transitions: transitions,
        data: data,
        setup: setup,
        teardown: teardown,
    };
})();
