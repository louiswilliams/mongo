/**
 * Overrides runCommand so operations that encounter the BackgroundOperationInProgressForNs/Db error
 * codes automatically retry.
 */
(function() {
    "use strict";

    const mongoRunCommandOriginal = Mongo.prototype.runCommand;
    const mongoRunCommandWithMetadataOriginal = Mongo.prototype.runCommandWithMetadata;

    const commandWhitelist = [
        "collMod",
        "compact",
        "convertToCapped",
        "createIndexes",
        "drop",
        "dropDatabase",
        "dropIndexes",
        "renameCollection",
    ];

    const timeout = 5 * 60 * 1000;
    const interval = 1000;

    function runCommandWithRetries(conn, dbName, commandObj, func, makeFuncArgs) {
        if (typeof commandObj !== "object" || commandObj === null) {
            return func.apply(conn, makeFuncArgs(commandObj));
        }

        const commandName = Object.keys(commandObj)[0];

        let res;

        assert.soon(() => {
            res = func.apply(conn, makeFuncArgs(commandObj));
            if (res.ok === 1) {
                return true;
            }

            // Commands that are not in the whitelist should never fail with this error code.
            if (!commandWhitelist.includes(commandName)) {
                return true;
            }

            // Only these are retryable.
            if (res.code !== ErrorCodes.BackgroundOperationInProgressForNamespace &&
                res.code !== ErrorCodes.BackgroundOperationInProgressForDatabase) {
                return true;
            }

            print("Retrying the '" + commandName +
                  "' command because a background operation is in progress.");

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
