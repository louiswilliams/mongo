'use strict';

/**
 * view_catalog_cycle_lookup.js
 *
 * Creates views which may include $lookup and $graphlookup stages and continually remaps those
 * views against other eachother and the underlying collection. We are looking to expose situations
 * where a $lookup or $graphLookup view that forms a cycle is created successfully.
 */

load('jstests/concurrency/fsm_workload_helpers/drop_utils.js');  // for dropCollections

var $config = (function() {

    // Use the workload name as a prefix for the view names, since the workload name is assumed
    // to be unique.
    const prefix = 'view_catalog_cycle_lookup_';

    var data = {
        viewList: ['viewA', 'viewB', 'viewC'].map(viewName => prefix + viewName),
        getRandomView: function(viewList) {
            return viewList[Random.randInt(viewList.length)];
        },
    };

    var states = (function() {
        /**
         * Redefines a view definition by changing the namespace it is a view on. This may lead to
         * a failed command if the given collMod would introduce a cycle. We ignore this error as it
         * is expected at view create/modification time.
         */
        function remapViewToView(db, collName) {
            const fromName = this.getRandomView(this.viewList.slice(0, 2));
            let toName;
            while (true) {
                toName = this.getRandomView(this.viewList);
                if (toName !== fromName) {
                    break;
                }
            }
            const res = db.runCommand({collMod: fromName, viewOn: toName, pipeline: []});
            assertAlways(res.ok === 1 || res.code === ErrorCodes.GraphContainsCycle, tojson(res));
            if (res.ok === 1) {
                jsTestLog("point " + fromName + " to " + toName);
            }
        }

        function readFromView(db, collName) {
            const viewName = this.getRandomView(this.viewList.slice(0, 2));
            assertAlways.commandWorked(db.runCommand({find: viewName}));
        }

        return {
            remapViewToView: remapViewToView,
            readFromView: readFromView,
        };

    })();

    var transitions = {
        remapViewToView: {remapViewToView: 0.00, readFromView: 1.00},
        readFromView: {remapViewToView: 1.00, readFromView: 0.00},
    };

    function setup(db, collName, cluster) {
        const coll = db[collName];

        assertAlways.writeOK(coll.insert({a: 1, b: 2}));
        assertAlways.writeOK(coll.insert({a: 2, b: 3}));
        assertAlways.writeOK(coll.insert({a: 3, b: 4}));
        assertAlways.writeOK(coll.insert({a: 4, b: 1}));

        for (let viewName of this.viewList) {
            assertAlways.commandWorked(db.createView(viewName, collName, []));
        }
    }

    function teardown(db, collName, cluster) {
        const pattern = new RegExp('^' + prefix + '[A-z]*$');
        // dropCollections(db, pattern);
    }

    return {
        threadCount: 100,
        iterations: 300,
        data: data,
        states: states,
        startState: 'readFromView',
        transitions: transitions,
        setup: setup,
        teardown: teardown,
    };
})();
