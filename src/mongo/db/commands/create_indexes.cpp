/**
 *    Copyright (C) 2013-2016 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include <string>
#include <vector>

#include "mongo/base/string_data.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/index_create.h"
#include "mongo/db/catalog/index_key_validate.h"
#include "mongo/db/client.h"
#include "mongo/db/command_generic_argument.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/op_observer.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/repl/replication_coordinator.h"
#include "mongo/db/s/collection_metadata.h"
#include "mongo/db/s/collection_sharding_state.h"
#include "mongo/db/s/database_sharding_state.h"
#include "mongo/db/server_options.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

using std::string;

using IndexVersion = IndexDescriptor::IndexVersion;

namespace {

const StringData kIndexesFieldName = "indexes"_sd;
const StringData kCommandName = "createIndexes"_sd;

/**
 * Parses the index specifications from 'cmdObj', validates them, and returns equivalent index
 * specifications that have any missing attributes filled in. If any index specification is
 * malformed, then an error status is returned.
 */
StatusWith<std::vector<BSONObj>> parseAndValidateIndexSpecs(
    OperationContext* opCtx,
    const NamespaceString& ns,
    const BSONObj& cmdObj,
    const ServerGlobalParams::FeatureCompatibility& featureCompatibility) {
    bool hasIndexesField = false;

    std::vector<BSONObj> indexSpecs;
    for (auto&& cmdElem : cmdObj) {
        auto cmdElemFieldName = cmdElem.fieldNameStringData();

        if (kIndexesFieldName == cmdElemFieldName) {
            if (cmdElem.type() != BSONType::Array) {
                return {ErrorCodes::TypeMismatch,
                        str::stream() << "The field '" << kIndexesFieldName
                                      << "' must be an array, but got "
                                      << typeName(cmdElem.type())};
            }

            for (auto&& indexesElem : cmdElem.Obj()) {
                if (indexesElem.type() != BSONType::Object) {
                    return {ErrorCodes::TypeMismatch,
                            str::stream() << "The elements of the '" << kIndexesFieldName
                                          << "' array must be objects, but got "
                                          << typeName(indexesElem.type())};
                }

                auto indexSpecStatus = index_key_validate::validateIndexSpec(
                    opCtx, indexesElem.Obj(), ns, featureCompatibility);
                if (!indexSpecStatus.isOK()) {
                    return indexSpecStatus.getStatus();
                }
                auto indexSpec = indexSpecStatus.getValue();

                if (IndexDescriptor::isIdIndexPattern(
                        indexSpec[IndexDescriptor::kKeyPatternFieldName].Obj())) {
                    auto status = index_key_validate::validateIdIndexSpec(indexSpec);
                    if (!status.isOK()) {
                        return status;
                    }
                } else if (indexSpec[IndexDescriptor::kIndexNameFieldName].String() == "_id_"_sd) {
                    return {ErrorCodes::BadValue,
                            str::stream() << "The index name '_id_' is reserved for the _id index, "
                                             "which must have key pattern {_id: 1}, found "
                                          << indexSpec[IndexDescriptor::kKeyPatternFieldName]};
                } else if (indexSpec[IndexDescriptor::kIndexNameFieldName].String() == "*"_sd) {
                    // An index named '*' cannot be dropped on its own, because a dropIndex oplog
                    // entry with a '*' as an index name means "drop all indexes in this
                    // collection".  We disallow creation of such indexes to avoid this conflict.
                    return {ErrorCodes::BadValue, "The index name '*' is not valid."};
                }

                indexSpecs.push_back(std::move(indexSpec));
            }

            hasIndexesField = true;
        } else if (kCommandName == cmdElemFieldName || isGenericArgument(cmdElemFieldName)) {
            continue;
        } else {
            return {ErrorCodes::BadValue,
                    str::stream() << "Invalid field specified for " << kCommandName << " command: "
                                  << cmdElemFieldName};
        }
    }

    if (!hasIndexesField) {
        return {ErrorCodes::FailedToParse,
                str::stream() << "The '" << kIndexesFieldName
                              << "' field is a required argument of the "
                              << kCommandName
                              << " command"};
    }

    if (indexSpecs.empty()) {
        return {ErrorCodes::BadValue, "Must specify at least one index to create"};
    }

    return indexSpecs;
}

/**
 * Returns index specifications with attributes (such as "collation") that are inherited from the
 * collection filled in.
 *
 * The returned index specifications will not be equivalent to the ones specified as 'indexSpecs' if
 * any missing attributes were filled in; however, the returned index specifications will match the
 * form stored in the IndexCatalog should any of these indexes already exist.
 */
StatusWith<std::vector<BSONObj>> resolveCollectionDefaultProperties(
    OperationContext* opCtx, const Collection* collection, std::vector<BSONObj> indexSpecs) {
    std::vector<BSONObj> indexSpecsWithDefaults = std::move(indexSpecs);

    for (size_t i = 0, numIndexSpecs = indexSpecsWithDefaults.size(); i < numIndexSpecs; ++i) {
        auto indexSpecStatus = index_key_validate::validateIndexSpecCollation(
            opCtx, indexSpecsWithDefaults[i], collection->getDefaultCollator());
        if (!indexSpecStatus.isOK()) {
            return indexSpecStatus.getStatus();
        }
        auto indexSpec = indexSpecStatus.getValue();

        if (IndexDescriptor::isIdIndexPattern(
                indexSpec[IndexDescriptor::kKeyPatternFieldName].Obj())) {
            std::unique_ptr<CollatorInterface> indexCollator;
            if (auto collationElem = indexSpec[IndexDescriptor::kCollationFieldName]) {
                auto collatorStatus = CollatorFactoryInterface::get(opCtx->getServiceContext())
                                          ->makeFromBSON(collationElem.Obj());
                // validateIndexSpecCollation() should have checked that the index collation spec is
                // valid.
                invariant(collatorStatus.getStatus());
                indexCollator = std::move(collatorStatus.getValue());
            }
            if (!CollatorInterface::collatorsMatch(collection->getDefaultCollator(),
                                                   indexCollator.get())) {
                return {ErrorCodes::BadValue,
                        str::stream() << "The _id index must have the same collation as the "
                                         "collection. Index collation: "
                                      << (indexCollator.get() ? indexCollator->getSpec().toBSON()
                                                              : CollationSpec::kSimpleSpec)
                                      << ", collection collation: "
                                      << (collection->getDefaultCollator()
                                              ? collection->getDefaultCollator()->getSpec().toBSON()
                                              : CollationSpec::kSimpleSpec)};
            }
        }

        indexSpecsWithDefaults[i] = indexSpec;
    }

    return indexSpecsWithDefaults;
}

}  // namespace

/**
 * { createIndexes : "bar", indexes : [ { ns : "test.bar", key : { x : 1 }, name: "x_1" } ] }
 */
class CmdCreateIndex : public ErrmsgCommandDeprecated {
public:
    CmdCreateIndex() : ErrmsgCommandDeprecated(kCommandName) {}

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }
    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    virtual Status checkAuthForCommand(Client* client,
                                       const std::string& dbname,
                                       const BSONObj& cmdObj) const {
        ActionSet actions;
        actions.addAction(ActionType::createIndex);
        Privilege p(parseResourcePattern(dbname, cmdObj), actions);
        if (AuthorizationSession::get(client)->isAuthorizedForPrivilege(p))
            return Status::OK();
        return Status(ErrorCodes::Unauthorized, "Unauthorized");
    }

    virtual bool errmsgRun(OperationContext* opCtx,
                           const string& dbname,
                           const BSONObj& cmdObj,
                           string& errmsg,
                           BSONObjBuilder& result) {
        const NamespaceString ns(CommandHelpers::parseNsCollectionRequired(dbname, cmdObj));

        Status status = userAllowedWriteNS(ns);
        uassertStatusOK(status);

        // Disallow users from creating new indexes on config.transactions since the sessions
        // code was optimized to not update indexes.
        uassert(ErrorCodes::IllegalOperation,
                str::stream() << "not allowed to create index on " << ns.ns(),
                ns != NamespaceString::kSessionTransactionsTableNamespace);

        auto specsWithStatus =
            parseAndValidateIndexSpecs(opCtx, ns, cmdObj, serverGlobalParams.featureCompatibility);
        uassertStatusOK(specsWithStatus.getStatus());
        auto specs = std::move(specsWithStatus.getValue());
        const size_t origSpecsSize = specs.size();

        // Before taking a strong database lock, first take a weaker collection lock to remove
        // index specs that  already exist. Only continue if new indexes need to be built.
        int numIndexesBefore = 0;
        {
            AutoGetCollection autoColl(opCtx, ns, MODE_IS);
            auto collection = autoColl.getCollection();
            if (collection) {
                auto indexSpecsWithDefaults =
                    resolveCollectionDefaultProperties(opCtx, collection, std::move(specs));
                uassertStatusOK(indexSpecsWithDefaults.getStatus());
                specs = std::move(indexSpecsWithDefaults.getValue());

                numIndexesBefore = collection->getIndexCatalog()->numIndexesTotal(opCtx);

                // Remove indexes that already exist.
                for (size_t i = 0; i < specs.size(); i++) {
                    Status status = collection->getIndexCatalog()
                                        ->prepareSpecForCreate(opCtx, specs[i])
                                        .getStatus();
                    if (status.code() == ErrorCodes::IndexAlreadyExists) {
                        specs.erase(specs.begin() + i);
                        i--;
                        continue;
                    }
                    uassertStatusOK(status);
                }
            }
        }
        result.append("numIndexesBefore", numIndexesBefore);
        if (specs.size() == 0) {
            result.append("numIndexesAfter", numIndexesBefore);
            result.append("note", "all indexes already exist");
            return true;
        }

        // now we know we have to create index(es)
        // Do not use AutoGetOrCreateDb because we may relock the DbLock in mode IX.
        Lock::DBLock dbLock(opCtx, ns.db(), MODE_X);
        if (!repl::ReplicationCoordinator::get(opCtx)->canAcceptWritesFor(opCtx, ns)) {
            uasserted(ErrorCodes::NotMaster,
                      str::stream() << "Not primary while creating indexes in " << ns.ns());
        }

        // Allow the strong lock acquisition above to be interrupted, but from this point forward do
        // not allow locks or re-locks to be interrupted.
        UninterruptibleLockGuard noInterrupt(opCtx->lockState());

        Database* db = DatabaseHolder::getDatabaseHolder().get(opCtx, ns.db());
        if (!db) {
            db = DatabaseHolder::getDatabaseHolder().openDb(opCtx, ns.db());
        }
        DatabaseShardingState::get(db).checkDbVersion(opCtx);

        Collection* collection = db->getCollection(opCtx, ns);
        if (collection) {
            result.appendBool("createdCollectionAutomatically", false);
        } else {
            if (db->getViewCatalog()->lookup(opCtx, ns.ns())) {
                errmsg = "Cannot create indexes on a view";
                uasserted(ErrorCodes::CommandNotSupportedOnView, errmsg);
            }

            status = userAllowedCreateNS(ns.db(), ns.coll());
            uassertStatusOK(status);

            writeConflictRetry(opCtx, kCommandName, ns.ns(), [&] {
                WriteUnitOfWork wunit(opCtx);
                collection = db->createCollection(opCtx, ns.ns(), CollectionOptions());
                invariant(collection);
                wunit.commit();
            });
            result.appendBool("createdCollectionAutomatically", true);
        }

        // Use AutoStatsTracker to update Top.
        boost::optional<AutoStatsTracker> statsTracker;
        const boost::optional<int> dbProfilingLevel = boost::none;
        statsTracker.emplace(opCtx, ns, Top::LockType::WriteLocked, dbProfilingLevel);

        MultiIndexBlock indexer(opCtx, collection);
        indexer.allowBackgroundBuilding();
        indexer.allowInterruption();

        indexer.removeExistingIndexes(&specs);

        if (specs.size() != origSpecsSize) {
            result.append("note", "index already exists");
        }

        for (size_t i = 0; i < specs.size(); i++) {
            const BSONObj& spec = specs[i];
            if (spec["unique"].trueValue()) {
                status = checkUniqueIndexConstraints(opCtx, ns, spec["key"].Obj());
                uassertStatusOK(status);
            }
        }

        std::vector<BSONObj> indexInfoObjs =
            writeConflictRetry(opCtx, kCommandName, ns.ns(), [&indexer, &specs] {
                return uassertStatusOK(indexer.init(specs));
            });

        // If we're a background index, replace exclusive db lock with an intent lock, so that
        // other readers and writers can proceed during this phase.
        if (indexer.getBuildInBackground()) {
            opCtx->recoveryUnit()->abandonSnapshot();
            dbLock.relockWithMode(MODE_IX);
            if (!repl::ReplicationCoordinator::get(opCtx)->canAcceptWritesFor(opCtx, ns)) {
                uasserted(ErrorCodes::NotMaster,
                          str::stream() << "Not primary while creating background indexes in "
                                        << ns.ns());
            }
        }

        try {
            Lock::CollectionLock colLock(opCtx->lockState(), ns.ns(), MODE_IX);
            uassertStatusOK(indexer.insertAllDocumentsInCollection());
        } catch (const DBException& e) {
            invariant(e.code() != ErrorCodes::WriteConflict);
            // Must have exclusive DB lock before we clean up the index build via the
            // destructor of 'indexer'.
            if (indexer.getBuildInBackground()) {
                try {
                    // This function cannot throw today, but we will preemptively prepare for
                    // that day, to avoid data corruption due to lack of index cleanup.
                    opCtx->recoveryUnit()->abandonSnapshot();
                    dbLock.relockWithMode(MODE_X);
                } catch (...) {
                    std::terminate();
                }
                uassert(ErrorCodes::NotMaster,
                        str::stream() << "Not primary while creating background indexes in "
                                      << ns.ns()
                                      << ": cleaning up index build failure due to "
                                      << e.toString(),
                        repl::ReplicationCoordinator::get(opCtx)->canAcceptWritesFor(opCtx, ns));
            }
            throw;
        }
        // Need to return db lock back to exclusive, to complete the index build.
        if (indexer.getBuildInBackground()) {
            opCtx->recoveryUnit()->abandonSnapshot();
            dbLock.relockWithMode(MODE_X);
            uassert(ErrorCodes::NotMaster,
                    str::stream() << "Not primary while completing index build in " << dbname,
                    repl::ReplicationCoordinator::get(opCtx)->canAcceptWritesFor(opCtx, ns));

            Database* db = DatabaseHolder::getDatabaseHolder().get(opCtx, ns.db());
            if (db) {
                DatabaseShardingState::get(db).checkDbVersion(opCtx);
            }

            uassert(28551, "database dropped during index build", db);
            uassert(28552, "collection dropped during index build", db->getCollection(opCtx, ns));
        }

        writeConflictRetry(opCtx, kCommandName, ns.ns(), [&] {
            WriteUnitOfWork wunit(opCtx);

            indexer.commit([opCtx, &ns, collection](const BSONObj& spec) {
                opCtx->getServiceContext()->getOpObserver()->onCreateIndex(
                    opCtx, ns, *(collection->uuid()), spec, false);
            });

            wunit.commit();
        });

        result.append("numIndexesAfter", collection->getIndexCatalog()->numIndexesTotal(opCtx));

        return true;
    }

private:
    static Status checkUniqueIndexConstraints(OperationContext* opCtx,
                                              const NamespaceString& nss,
                                              const BSONObj& newIdxKey) {
        invariant(opCtx->lockState()->isCollectionLockedForMode(nss.ns(), MODE_X));

        auto metadata = CollectionShardingState::get(opCtx, nss)->getMetadata(opCtx);
        if (metadata->isSharded()) {
            ShardKeyPattern shardKeyPattern(metadata->getKeyPattern());
            if (!shardKeyPattern.isUniqueIndexCompatible(newIdxKey)) {
                return Status(ErrorCodes::CannotCreateIndex,
                              str::stream() << "cannot create unique index over " << newIdxKey
                                            << " with shard key pattern "
                                            << shardKeyPattern.toBSON());
            }
        }


        return Status::OK();
    }
} cmdCreateIndex;
}
