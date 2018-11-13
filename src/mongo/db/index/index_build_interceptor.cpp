/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kIndex

#include "mongo/platform/basic.h"

#include "mongo/db/index/index_build_interceptor.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/curop.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/multi_key_path_tracker.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/s/operation_sharding_state.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/uuid.h"

namespace mongo {

namespace {
const bool makeCollections = true;
}

NamespaceString IndexBuildInterceptor::makeTempSideWritesNs() {
    return NamespaceString("local.system.sideWrites-" + UUID::gen().toString());
}

void IndexBuildInterceptor::ensureSideWritesCollectionExists(OperationContext* opCtx) {
    if (!makeCollections) {
        return;
    }

    // TODO SERVER-38027 Consider pushing this higher into the createIndexes command logic.
    OperationShardingState::get(opCtx).setAllowImplicitCollectionCreation(BSONElement());

    AutoGetOrCreateDb local(opCtx, "local", LockMode::MODE_X);
    CollectionOptions options;
    options.setNoIdIndex();
    options.temp = true;

    local.getDb()->createCollection(opCtx, _sideWritesNs.ns(), options);
}

void IndexBuildInterceptor::removeSideWritesCollection(OperationContext* opCtx) {
    if (!makeCollections) {
        return;
    }

    AutoGetDb local(opCtx, "local", LockMode::MODE_X);
    fassert(50994, local.getDb()->dropCollectionEvenIfSystem(opCtx, _sideWritesNs, repl::OpTime()));
}

Status IndexBuildInterceptor::drainWritesIntoIndex(OperationContext* opCtx,
                                                   IndexAccessMethod* indexAccessMethod,
                                                   const InsertDeleteOptions& options,
                                                   ScanYield scanYield) {
    invariant(opCtx->lockState()->inAWriteUnitOfWork());

    // TODO: Deal with duplicates

    // Only drain writes up to the last visible record
    RecordId applyToRecord = _peekAtLastRecord(opCtx);

    // Collection is empty.
    if (applyToRecord.isNull())
        return Status::OK();

    // There are no records to apply.
    if (_lastAppliedRecord == applyToRecord)
        return Status::OK();

    LOG(0) << "draining writes from record " << _lastAppliedRecord.repr() << " to "
           << applyToRecord.repr();

    AutoGetCollection autoColl(opCtx, _sideWritesNs, LockMode::MODE_IS);
    invariant(autoColl.getCollection());

    // Callers that are holding strong locks may not want to yield.
    auto yieldPolicy = (scanYield = ScanYield::kInterruptOnly)
        ? PlanExecutor::YieldPolicy::INTERRUPT_ONLY
        : PlanExecutor::YieldPolicy::YIELD_AUTO;

    // This collection scan is resumable at the _lastAppliedRecord.
    auto collection = autoColl.getCollection();
    auto collScan = InternalPlanner::collectionScan(opCtx,
                                                    collection->ns().ns(),
                                                    collection,
                                                    yieldPolicy,
                                                    InternalPlanner::FORWARD,
                                                    _lastAppliedRecord);

    // These are used for logging only.
    int64_t totalDeleted = 0;
    int64_t totalInserted = 0;

    // In the case of an unnexpected WriteConflict or other exception that causes inserts to roll
    // back, reset the counters back to what they were at the start.
    const int64_t appliedAtStart = _numApplied;
    const RecordId lastAppliedAtStart = _lastAppliedRecord;
    opCtx->recoveryUnit()->onRollback([&] {
        _numApplied = appliedAtStart;
        _lastAppliedRecord = lastAppliedAtStart;
    });

    // Setup the progress meter.
    static const char* curopMessage = "Index build draining writes";
    stdx::unique_lock<Client> lk(*opCtx->getClient());
    ProgressMeterHolder progress(CurOp::get(opCtx)->setMessage_inlock(
        curopMessage, curopMessage, _sideWritesCounter.load() - appliedAtStart, 1));
    lk.unlock();


    BSONObj operation;
    RecordId currentRecord;
    PlanExecutor::ExecState state;

    while (PlanExecutor::ExecState::ADVANCED ==
           (state = collScan->getNext(&operation, &currentRecord))) {

        // Seeking may result in seeing the same document when resuming a scan.
        if (currentRecord == _lastAppliedRecord)
            continue;

        const BSONObj key = operation["key"].Obj();
        const RecordId opRecordId = RecordId(operation["recordId"].Long());
        const Op opType =
            (operation.getStringField("op") == std::string("i")) ? Op::kInsert : Op::kDelete;
        const BSONObjSet keySet = SimpleBSONObjComparator::kInstance.makeBSONObjSet({key});

        if (opType == Op::kInsert) {
            InsertResult result;
            Status s =
                indexAccessMethod->insertKeys(opCtx,
                                              keySet,
                                              SimpleBSONObjComparator::kInstance.makeBSONObjSet(),
                                              _multikeyPaths.get_value_or({}),
                                              opRecordId,
                                              options,
                                              &result);
            if (!s.isOK()) {
                return s;
            }

            invariant(!result.dupsInserted.size());
            totalInserted += result.numInserted;
        } else {
            invariant(opType == Op::kDelete);

            int64_t numDeleted;
            Status s =
                indexAccessMethod->removeKeys(opCtx, keySet, opRecordId, options, &numDeleted);
            if (!s.isOK()) {
                return s;
            }

            totalDeleted += numDeleted;
        }

        progress->hit();

        // Since drain is resumable, keep track of which records we have applied.
        _lastAppliedRecord = currentRecord;
        _numApplied++;

        // Stop after inserting the last record we know has no "holes" before it.
        if (currentRecord == applyToRecord)
            break;
    }

    progress->finished();

    LOG(0) << "applied " << (_numApplied - appliedAtStart) << " side writes. i: " << totalInserted
           << ", d: " << totalDeleted << ", total: " << _numApplied;

    if (PlanExecutor::ADVANCED == state) {
        invariant(currentRecord == applyToRecord);
        return Status::OK();
    }

    if (PlanExecutor::IS_EOF != state) {
        return WorkingSetCommon::getMemberObjectStatus(operation);
    }
    return Status::OK();
}

bool IndexBuildInterceptor::areAllWritesApplied(OperationContext* opCtx) const {

    RecordId lastRecord = _peekAtLastRecord(opCtx);
    return lastRecord == _lastAppliedRecord;
}

RecordId IndexBuildInterceptor::_peekAtLastRecord(OperationContext* opCtx) const {

    // Stop writes on the side writes collection to look at the last record. By stopping writes on
    // the side collection, this will prevent seeing "holes" of writes with lower RecordIds that are
    // not visible in this snapshot.
    AutoGetCollection autoColl(opCtx, _sideWritesNs, LockMode::MODE_S);
    invariant(autoColl.getCollection());

    auto cursor = autoColl.getCollection()->getCursor(opCtx, false /* forward */);
    auto record = cursor->next();

    // The collection is empty.
    if (!record)
        return RecordId();

    return record->id;
}

Status IndexBuildInterceptor::sideWrite(OperationContext* opCtx,
                                        IndexAccessMethod* indexAccessMethod,
                                        const BSONObj* obj,
                                        RecordId loc,
                                        Op op,
                                        int64_t* numKeysOut) {
    *numKeysOut = 0;
    BSONObjSet keys = SimpleBSONObjComparator::kInstance.makeBSONObjSet();
    BSONObjSet multikeyMetadataKeys = SimpleBSONObjComparator::kInstance.makeBSONObjSet();
    MultikeyPaths multikeyPaths;

    indexAccessMethod->getKeys(*obj,
                               IndexAccessMethod::GetKeysMode::kEnforceConstraints,
                               &keys,
                               &multikeyMetadataKeys,
                               &multikeyPaths);
    // Maintain parity with IndexAccessMethods handling of key counting. Only include
    // `multikeyMetadataKeys` when inserting.
    *numKeysOut = keys.size() + (op == Op::kInsert ? multikeyMetadataKeys.size() : 0);

    if (_multikeyPaths) {
        MultikeyPathTracker::mergeMultikeyPaths(&_multikeyPaths.get(), multikeyPaths);
    } else {
        // `mergeMultikeyPaths` is sensitive to the two inputs having the same multikey
        // "shape". Initialize `_multikeyPaths` with the right shape from the first result.
        _multikeyPaths = multikeyPaths;
    }

    AutoGetCollection coll(opCtx, _sideWritesNs, LockMode::MODE_IX);
    invariant(coll.getCollection());

    std::vector<InsertStatement> toInsert;
    for (const auto& key : keys) {
        // Documents inserted into this table must be consumed in insert-order. Today, we can rely
        // on storage engines to return documents in insert-order, but with clustered indexes,
        // that may no longer be true.
        //
        // Additionally, these writes should be timestamped with the same timestamps that the
        // other writes making up this operation are given. When index builds can cope with
        // replication rollbacks, side table writes associated with a CUD operation should
        // remain/rollback along with the corresponding oplog entry.
        toInsert.emplace_back(BSON(
            "op" << (op == Op::kInsert ? "i" : "d") << "key" << key << "recordId" << loc.repr()));
    }

    if (op == Op::kInsert) {
        // Wildcard indexes write multikey path information, typically part of the catalog
        // document, to the index itself. Multikey information is never deleted, so we only need
        // to add this data on the insert path.
        for (const auto& key : multikeyMetadataKeys) {
            toInsert.emplace_back(BSON("op"
                                       << "i"
                                       << "key"
                                       << key
                                       << "recordId"
                                       << static_cast<int64_t>(
                                              RecordId::ReservedId::kWildcardMultikeyMetadataId)));
        }
    }

    _sideWritesCounter.fetchAndAdd(toInsert.size());

    // This is necessary for operations in transactions to not generate operations for each insert.
    repl::UnreplicatedWritesBlock urwb(opCtx);

    OpDebug* const opDebug = nullptr;
    const bool fromMigrate = false;
    return coll.getCollection()->insertDocuments(
        opCtx, toInsert.begin(), toInsert.end(), opDebug, fromMigrate);
}
}  // namespace mongo
