/**
 * Overrides runCommand so that background index build are causally consistent.
 * TODO: SERVER-? This override is not necessary when two-phase index builds are complete.
 */
(function() {
    "use strict";

    const mongoRunCommandOriginal = Mongo.prototype.runCommand;
    const mongoRunCommandWithMetadataOriginal = Mongo.prototype.runCommandWithMetadata;

    // This override runs a collMod after a createIndex command. After collMod completes
    // we can guarantee the background index build has also completed until that time. We update the
    // command response operationTime and $clusterTime so causally consistent reads only read from
    // that point onwards.
    function runCommandWithCollMod(conn, dbName, commandObj, func, makeFuncArgs) {
        if (typeof commandObj !== "object" || commandObj === null) {
            return func.apply(conn, makeFuncArgs(commandObj));
        }

        const commandName = Object.keys(commandObj)[0];

        let res = func.apply(conn, makeFuncArgs(commandObj));
        if (commandName != "createIndex") {
            return res;
        }
        if (!res.ok) {
            return res;
        }

        let collModRes = conn.getDB(dbName).runCommand(
            {collMod: commandObj[commandName], usePowerOf2Sizes: true});

        try {
            assert.commandWorked(collModRes);
        } catch (e) {
            // If the namespace is droppped by a background thread after the index is created but
            // before the collMod is run it is possible to encounter a NamespaceNotFound error.
            if (e.code != ErrorCodes.NamespaceNotFound)
                throw e;
        }

        // Overwrite the createIndex command's operation and cluster times, so that the owning
        // session can perform causal reads.
        if (collModRes.hasOwnProperty("operationTime")) {
            res.operationTime = collModRes["operationTime"];
        }
        if (collModRes.hasOwnProperty("$clusterTime")) {
            res.clusterTime = collModRes["$clusterTime"];
        }
        return res;
    }

    Mongo.prototype.runCommand = function(dbName, commandObj, options) {
        return runCommandWithCollMod(this,
                                     dbName,
                                     commandObj,
                                     mongoRunCommandOriginal,
                                     (commandObj) => [dbName, commandObj, options]);
    };

    Mongo.prototype.runCommandWithMetadata = function(dbName, metadata, commandArgs) {
        return runCommandWithCollMod(this,
                                     dbName,
                                     commandArgs,
                                     mongoRunCommandWithMetadataOriginal,
                                     (commandArgs) => [dbName, metadata, commandArgs]);
    };
})();
