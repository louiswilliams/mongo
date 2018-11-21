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
#include "mongo/db/service_context.h"
#include "mongo/util/log.h"
#include "mongo/util/progress_meter.h"
#include "mongo/util/uuid.h"

namespace mongo {

void IndexBuildInterceptor::ensureTempSideWritesTable(OperationContext* opCtx) {
    _sideWritesTable =
        opCtx->getServiceContext()->getStorageEngine()->makeTemporaryRecordStore(opCtx);
}

void IndexBuildInterceptor::removeTempSideWritesTable(OperationContext* opCtx) {}

Status IndexBuildInterceptor::drainWritesIntoIndex(OperationContext* opCtx,
                                                   IndexAccessMethod* indexAccessMethod,
                                                   const InsertDeleteOptions& options,
                                                   ScanYield scanYield) {
    invariant(!opCtx->lockState()->inAWriteUnitOfWork());

    // These are used for logging only.
    int64_t totalDeleted = 0;
    int64_t totalInserted = 0;

    const int64_t appliedAtStart = _numApplied;

    // Setup the progress meter.
    static const char* curopMessage = "Index build draining writes";
    stdx::unique_lock<Client> lk(*opCtx->getClient());
    ProgressMeterHolder progress(CurOp::get(opCtx)->setMessage_inlock(
        curopMessage, curopMessage, _sideWritesCounter.load() - appliedAtStart, 1));
    lk.unlock();

    // Buffer operations into a batches to insert per WriteUnitOfWork. Impose an upper limit on the
    // number of documents and the total size of the batch.
    const int32_t kBatchMaxSize = 1000;
    const int64_t kBatchMaxBytes = BSONObjMaxInternalSize;

    int64_t batchSizeBytes = 0;

    std::vector<SideWriteRecord> batch;
    batch.reserve(kBatchMaxSize);

    // Hold on to documents that would exceed the per-batch memory limit. Always insert this first
    // into the next batch.
    boost::optional<SideWriteRecord> stashed;

    auto cursor = _sideWritesTable->getCursor(opCtx);

    bool atEof = false;
    while (!atEof) {

        // Stashed recordsshould be inserted into a batch first.
        if (stashed) {
            invariant(batch.empty());
            batch.push_back(std::move(stashed.get()));
            stashed.reset();
        }

        auto record = cursor->next();

        if (record) {
            RecordId currentRecordId = record->id;
            BSONObj docOut = record->data.releaseToBson();
            // If the total batch size in bytes would be too large, stash this document and let the
            // current batch insert.
            int objSize = docOut.objsize();
            if (batchSizeBytes + objSize > kBatchMaxBytes) {
                invariant(!stashed);

                // Stash this document to be inserted in the next batch.
                stashed.emplace(currentRecordId, docOut);
            } else {
                batchSizeBytes += objSize;
                batch.emplace_back(currentRecordId, docOut);

                // Continue if there is more room in the batch.
                if (batch.size() < kBatchMaxSize) {
                    continue;
                }
            }
        } else {
            atEof = true;
            if (batch.empty())
                break;
        }

        invariant(!batch.empty());

        // If we are here, either we have reached the end of the collection or the batch is full, so
        // insert everything in one WriteUnitOfWork, and delete each inserted document from the side
        // writes table.
        WriteUnitOfWork wuow(opCtx);
        for (auto& operation : batch) {
            auto status = _applyWrite(
                opCtx, indexAccessMethod, operation.second, options, &totalInserted, &totalDeleted);
            if (!status.isOK()) {
                return status;
            }

            // Delete the document from the collection as soon as it has beenn inserted into the
            // index. This ensures that no key is every inserted twice and no keys are skipped.
            _sideWritesTable->deleteRecord(opCtx, operation.first);
        }
        cursor->save();
        wuow.commit();

        cursor->restore();

        progress->hit(batch.size());
        _numApplied += batch.size();
        batch.clear();
        batchSizeBytes = 0;
    }

    progress->finished();

    log() << "applied " << (_numApplied - appliedAtStart) << " side writes. i: " << totalInserted
          << ", d: " << totalDeleted << ", total: " << _numApplied;

    return Status::OK();
}

Status IndexBuildInterceptor::_applyWrite(OperationContext* opCtx,
                                          IndexAccessMethod* indexAccessMethod,
                                          const BSONObj& operation,
                                          const InsertDeleteOptions& options,
                                          int64_t* const keysInserted,
                                          int64_t* const keysDeleted) {
    const BSONObj key = operation["key"].Obj();
    const RecordId opRecordId = RecordId(operation["recordId"].Long());
    const Op opType =
        (strcmp(operation.getStringField("op"), "i") == 0) ? Op::kInsert : Op::kDelete;
    const BSONObjSet keySet = SimpleBSONObjComparator::kInstance.makeBSONObjSet({key});

    if (opType == Op::kInsert) {

        InsertResult result;
        Status s =
            indexAccessMethod->insertKeys(opCtx,
                                          keySet,
                                          SimpleBSONObjComparator::kInstance.makeBSONObjSet(),
                                          MultikeyPaths{},
                                          opRecordId,
                                          options,
                                          &result);
        if (!s.isOK()) {
            return s;
        }

        invariant(!result.dupsInserted.size());
        *keysInserted += result.numInserted;
    } else {
        invariant(opType == Op::kDelete);
        DEV invariant(strcmp(operation.getStringField("op"), "d") == 0);

        int64_t numDeleted;
        Status s = indexAccessMethod->removeKeys(opCtx, keySet, opRecordId, options, &numDeleted);
        if (!s.isOK()) {
            return s;
        }

        *keysDeleted += numDeleted;
    }
    return Status::OK();
}

bool IndexBuildInterceptor::areAllWritesApplied(OperationContext* opCtx) const {
    invariant(_sideWritesTable);
    auto cursor = _sideWritesTable->getCursor(opCtx, false /* forward */);
    auto record = cursor->next();

    // The collection is empty only when all writes are applied.
    if (!record)
        return true;

    return false;
}

boost::optional<MultikeyPaths> IndexBuildInterceptor::getMultikeyPaths() const {
    stdx::unique_lock<stdx::mutex> lk(_multikeyPathMutex);
    return _multikeyPaths;
}

Status IndexBuildInterceptor::sideWrite(OperationContext* opCtx,
                                        IndexAccessMethod* indexAccessMethod,
                                        const BSONObj* obj,
                                        RecordId loc,
                                        Op op,
                                        int64_t* const numKeysOut) {
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

    if (*numKeysOut == 0) {
        return Status::OK();
    }

    {
        stdx::unique_lock<stdx::mutex> lk(_multikeyPathMutex);
        if (_multikeyPaths) {
            MultikeyPathTracker::mergeMultikeyPaths(&_multikeyPaths.get(), multikeyPaths);
        } else {
            // `mergeMultikeyPaths` is sensitive to the two inputs having the same multikey
            // "shape". Initialize `_multikeyPaths` with the right shape from the first result.
            _multikeyPaths = multikeyPaths;
        }
    }

    std::vector<BSONObj> toInsert;
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
    opCtx->recoveryUnit()->onRollback(
        [=] { _sideWritesCounter.fetchAndSubtract(toInsert.size()); });

    std::vector<Record> records;
    for (auto& obj : toInsert) {
        records.emplace_back(Record{RecordId(), RecordData(obj.objdata(), obj.objsize())});
    }

    std::vector<Timestamp> timestamps;
    timestamps.reserve(toInsert.size());
    return _sideWritesTable->insertRecords(opCtx, &records, timestamps);
}
}  // namespace mongo
