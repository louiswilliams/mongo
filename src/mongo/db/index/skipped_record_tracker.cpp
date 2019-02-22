/**
 *    Copyright (C) 2019-present MongoDB, Inc.
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

#include "mongo/db/index/skipped_record_tracker.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_build_interceptor.h"
#include "mongo/util/log.h"

namespace mongo {

SkippedRecordTracker::SkippedRecordTracker(OperationContext* opCtx,
                                           IndexBuildInterceptor* interceptor,
                                           IndexCatalogEntry* indexCatalogEntry)
    : _interceptor(interceptor),
      _indexCatalogEntry(indexCatalogEntry),
      _skippedRecordsTable(
          opCtx->getServiceContext()->getStorageEngine()->makeTemporaryRecordStore(opCtx)) {}

Status SkippedRecordTracker::record(OperationContext* opCtx, const RecordId& recordId) {
    log() << "skipping indexing error for record " << recordId;
    auto toInsert = BSON("recordId" << recordId.repr());
    return _skippedRecordsTable->rs()
        ->insertRecord(opCtx, toInsert.objdata(), toInsert.objsize(), Timestamp::min())
        .getStatus();
}

Status SkippedRecordTracker::retrySkippedRecords(OperationContext* opCtx,
                                                 const Collection* collection) {
    InsertDeleteOptions options;
    collection->getIndexCatalog()->prepareInsertDeleteOptions(
        opCtx, _indexCatalogEntry->descriptor(), &options);

    // This should only be called when constraints are being enforced, on a primary. It does not
    // make sense, nor is it necessary for this to be called on a secondary.
    invariant(options.getKeysMode == IndexAccessMethod::GetKeysMode::kEnforceConstraints);

    auto cursor = _skippedRecordsTable->rs()->getCursor(opCtx);
    while (auto record = cursor->next()) {
        const BSONObj doc = record->data.toBson();

        // This is the RecordId of the skipped record. We don't care about the RecordId of the item
        // in the temp table.
        const RecordId recordId(doc["recordId"].Long());

        WriteUnitOfWork wuow(opCtx);

        // If the record still exists, get a potentially new version of the document to index.
        auto collCursor = collection->getCursor(opCtx);
        if (auto skippedRecord = collCursor->seekExact(recordId)) {
            const auto docBson = skippedRecord->data.toBson();

            try {
                // Because constraint enforcement is set, this will throw if there are any indexing
                // errors, instead of writing back to the skipped records table, which would
                // normally happen if constraints were relaxed.
                int64_t unused;
                auto status = _interceptor->sideWrite(opCtx,
                                                      _indexCatalogEntry->accessMethod(),
                                                      &docBson,
                                                      options,
                                                      recordId,
                                                      IndexBuildInterceptor::Op::kInsert,
                                                      &unused);
                if (!status.isOK()) {
                    return status;
                }

            } catch (const DBException& ex) {
                return ex.toStatus();
            }
        }

        // Delete the record so it is not applied more than once.
        _skippedRecordsTable->rs()->deleteRecord(opCtx, record->id);

        cursor->save();
        wuow.commit();
        cursor->restore();
    }

    return Status::OK();
}

bool SkippedRecordTracker::areAllSkippedRecordsApplied(OperationContext* opCtx) const {
    auto cursor = _skippedRecordsTable->rs()->getCursor(opCtx);
    auto record = cursor->next();

    // The table is empty only when all writes are applied.
    if (!record)
        return true;

    return false;
}

}  // mongo
