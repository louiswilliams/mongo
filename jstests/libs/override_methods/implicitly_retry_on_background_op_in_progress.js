/**
 * Overrides runCommand so operations that encounter the BackgroundOperationInProgressForNs/Db error
 * codes automatically retry.
 */
(function() {
    "use strict";

    const mongoRunCommandOriginal = Mongo.prototype.runCommand;
    const mongoRunCommandWithMetadataOriginal = Mongo.prototype.runCommandWithMetadata;

    // These are all commands that can return BackgroundOperationInProgress error codes.
    const commandWhitelist = new Set([
        "collMod",
        "compact",
        "convertToCapped",
        "createIndexes",
        "drop",
        "dropDatabase",
        "dropIndexes",
        "renameCollection",
    ]);

    // Whitelisted errors a command may encounter when retried on a sharded cluster. Shards may
    // return different responses, so errors associated with repeated executions of a command may be
    // ignored.
    const acceptableCommandErrors = {
        "drop": [ErrorCodes.NamespaceNotFound],
        "dropIndexes": [ErrorCodes.IndexNotFound],
        "renameCollection": [ErrorCodes.NamespaceNotFound],
    };

    const timeout = 5 * 60 * 1000;
    const interval = 1000;

    function hasBackgroundOpInProgress(res) {
        // Only these are retryable.
        return res.code === ErrorCodes.BackgroundOperationInProgressForNamespace ||
            res.code === ErrorCodes.BackgroundOperationInProgressForDatabase;
    }

    function runCommandWithRetries(conn, dbName, commandObj, func, makeFuncArgs) {
        if (typeof commandObj !== "object" || commandObj === null) {
            return func.apply(conn, makeFuncArgs(commandObj));
        }

        const commandName = Object.keys(commandObj)[0];

        let res;
        let attempt = 0;

        assert.soon(() => {
            attempt++;

            res = func.apply(conn, makeFuncArgs(commandObj));
            if (res.ok === 1) {
                return true;
            }

            // Commands that are not in the whitelist should never fail with this error code.
            if (!commandWhitelist.has(commandName)) {
                return true;
            }

            let message = "Retrying the '" + commandName +
                "' command because a background operation is in progress (attempt " + attempt + ")";

            // This is the code path for a replica set, standalone, or mongos where all shards
            // returned the same error.
            if (hasBackgroundOpInProgress(res)) {
                print(message);
                return false;
            }

            // In certain cases, retrying a command on a sharded cluster may result in a scenario
            // where one shard has executed the command and another still has a background operation
            // in progress. Retry, ignoring whitelisted errors on a command-by-command basis.
            if (conn.isMongos()) {
                let shardsWithBackgroundOps = [];

                // If any shard has a background operation in progress and the other shards sent
                // whitelisted errors after a first attempt, retry the entire command.
                for (let shard in res.raw) {
                    let shardRes = res.raw[shard];

                    if (hasBackgroundOpInProgress(shardRes)) {
                        shardsWithBackgroundOps.push(field);
                    }

                    // If any of the shards return an error that is not whitelisted or even if a
                    // whitelisted error is received on the first attempt, do not retry.
                    let acceptableErrors = acceptableCommandErrors[commandName] || [];
                    if (!acceptableErrors.includes(shardRes.code) || attempt == 1) {
                        return true;
                    }
                }

                if (shardsWithBackgroundOps.length === 0) {
                    return true;
                }

                print(message + " on shards: " + tojson(shardsWithBackgroundOps));
                return false;
            }

        }, "Timed out while retrying command: " + tojson(res), timeout, interval);
        return res;
    }

    Mongo.prototype.runCommand = function(dbName, commandObj, options) {
        return runCommandWithRetries(this,
                                     dbName,
                                     commandObj,
                                     mongoRunCommandOriginal,
                                     (commandObj) => [dbName, commandObj, options]);
    };

    Mongo.prototype.runCommandWithMetadata = function(dbName, metadata, commandArgs) {
        return runCommandWithRetries(this,
                                     dbName,
                                     commandArgs,
                                     mongoRunCommandWithMetadataOriginal,
                                     (commandArgs) => [dbName, metadata, commandArgs]);
    };
})();
