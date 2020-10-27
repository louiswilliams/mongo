/**
 *    Copyright (C) 2020-present MongoDB, Inc.
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

#pragma once

#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_build_interceptor.h"

namespace mongo {

class Collection;
class CollectionPtr;
class CollectionWriter;
class MatchExpression;
class NamespaceString;
class OperationContext;

class IndexBuilderInterface {
public:
    virtual ~IndexBuilderInterface(){};
    /**
     * By default we enforce the 'unique' flag in specs when building an index by failing.
     * If this is called before init(), we will ignore unique violations. This has no effect if
     * no specs are unique.
     *
     * If this is called, any 'dupRecords' set passed to dumpInsertsFromBulk() will never be
     * filled.
     */
    virtual void ignoreUniqueConstraint() = 0;

    /**
     * Sets an index build UUID associated with the indexes for this builder. This call is required
     * for two-phase index builds.
     */
    virtual void setTwoPhaseBuildUUID(UUID indexBuildUUID) = 0;

    /**
     * Prepares the index(es) for building and returns the canonicalized form of the requested index
     * specifications.
     *
     * Calls 'onInitFn' in the same WriteUnitOfWork as the 'ready: false' write to the index after
     * all indexes have been initialized. For callers that timestamp this write, use
     * 'makeTimestampedIndexOnInitFn', otherwise use 'kNoopOnInitFn'.
     *
     * Does not need to be called inside of a WriteUnitOfWork (but can be due to nesting).
     *
     * Requires holding an exclusive lock on the collection.
     */
    using OnInitFn = std::function<Status(std::vector<BSONObj>& specs)>;
    virtual StatusWith<std::vector<BSONObj>> init(
        OperationContext* opCtx,
        CollectionWriter& collection,
        const std::vector<BSONObj>& specs,
        OnInitFn onInit,
        const boost::optional<ResumeIndexInfo>& resumeInfo = boost::none) = 0;
    virtual StatusWith<std::vector<BSONObj>> init(OperationContext* opCtx,
                                                  CollectionWriter& collection,
                                                  const BSONObj& spec,
                                                  OnInitFn onInit) = 0;

    /**
     * Inserts all documents in the Collection into the indexes and logs with timing info.
     *
     * This is a replacement for calling both insertSingleDocumentForInitialSyncOrRecovery and
     * dumpInsertsFromBulk. Do not call this if you are calling either of them.
     *
     * Will fail if violators of uniqueness constraints exist.
     *
     * Can throw an exception if interrupted.
     *
     * Should not be called inside of a WriteUnitOfWork.
     */
    virtual Status insertAllDocumentsInCollection(
        OperationContext* opCtx,
        const CollectionPtr& collection,
        boost::optional<RecordId> resumeAfterRecordId = boost::none) = 0;

    /**
     * Call this after init() for each document in the collection.
     *
     * Do not call if you called insertAllDocumentsInCollection();
     *
     * Should be called inside of a WriteUnitOfWork.
     */
    virtual Status insertSingleDocumentForInitialSyncOrRecovery(OperationContext* opCtx,
                                                                const BSONObj& wholeDocument,
                                                                const RecordId& loc) = 0;

    /**
     * Call this after the last insertSingleDocumentForInitialSyncOrRecovery(). This gives the index
     * builder a chance to do any long-running operations in separate units of work from commit().
     *
     * Do not call if you called insertAllDocumentsInCollection();
     *
     * If 'onDuplicateRecord' is passed as non-NULL and duplicates are not allowed for the index,
     * violators of uniqueness constraints will be handled by 'onDuplicateRecord'.
     *
     * Should not be called inside of a WriteUnitOfWork.
     */
    virtual Status dumpInsertsFromBulk(OperationContext* opCtx,
                                       const CollectionPtr& collection) = 0;
    virtual Status dumpInsertsFromBulk(
        OperationContext* opCtx,
        const CollectionPtr& collection,
        const IndexAccessMethod::RecordIdHandlerFn& onDuplicateRecord) = 0;
    /**
     * For background indexes using an IndexBuildInterceptor to capture inserts during a build,
     * drain these writes into the index. If intent locks are held on the collection, more writes
     * may come in after this drain completes. To ensure that all writes are completely drained
     * before calling commit(), stop writes on the collection by holding a S or X while calling this
     * method.
     *
     * When 'readSource' is not kUnset, perform the drain by reading at the timestamp described by
     * the ReadSource.
     *
     * Must not be in a WriteUnitOfWork.
     */
    virtual Status drainBackgroundWrites(
        OperationContext* opCtx,
        RecoveryUnit::ReadSource readSource,
        IndexBuildInterceptor::DrainYieldPolicy drainYieldPolicy) = 0;


    /**
     * Retries key generation and insertion for all records skipped during the collection scanning
     * phase.
     *
     * Index builds ignore key generation errors on secondaries. In steady-state replication, all
     * writes from the primary are eventually applied, so an index build should always succeed when
     * the primary commits. In two-phase index builds, a secondary may become primary in the middle
     * of an index build, so it must ensure that before it finishes, it has indexed all documents in
     * a collection, requiring a call to this function upon completion.
     */
    virtual Status retrySkippedRecords(OperationContext* opCtx,
                                       const CollectionPtr& collection) = 0;

    /**
     * Check any constraits that may have been temporarily violated during the index build for
     * background indexes using an IndexBuildInterceptor to capture writes. The caller is
     * responsible for ensuring that all writes on the collection are visible.
     *
     * Must not be in a WriteUnitOfWork.
     */
    virtual Status checkConstraints(OperationContext* opCtx, const CollectionPtr& collection) = 0;

    /**
     * Marks the index ready for use. Should only be called as the last method after
     * dumpInsertsFromBulk() or insertAllDocumentsInCollection() return success.
     *
     * Should be called inside of a WriteUnitOfWork. If the index building is to be logOp'd,
     * logOp() should be called from the same unit of work as commit().
     *
     * `onCreateEach` will be called after each index has been marked as "ready".
     * `onCommit` will be called after all indexes have been marked "ready".
     *
     * Requires holding an exclusive lock on the collection.
     */
    using OnCommitFn = std::function<void()>;
    using OnCreateEachFn = std::function<void(const BSONObj& spec)>;
    virtual Status commit(OperationContext* opCtx,
                          Collection* collection,
                          OnCreateEachFn onCreateEach,
                          OnCommitFn onCommit) = 0;

    /**
     * Ensures the index build state is cleared correctly after index build failure.
     *
     * Must be called before object destruction if init() has been called; and safe to call if
     * init() has not been called.
     *
     * By only requiring this call after init(), we allow owners of the object to exit without
     * further handling if they never use the object.
     *
     * `onCleanUp` will be called after all indexes have been removed from the catalog.
     */
    using OnCleanUpFn = std::function<void()>;
    virtual void abortIndexBuild(OperationContext* opCtx,
                                 CollectionWriter& collection,
                                 OnCleanUpFn onCleanUp) noexcept = 0;

    /**
     * May be called at any time after construction but before a successful commit(). Suppresses
     * the default behavior on destruction of removing all traces of uncommitted index builds.
     * May delete internal tables, but this is not transactional. Writes the resumable index
     * build state to disk if resumable index builds are supported.
     *
     * This should only be used during shutdown or rollback.
     */
    virtual void abortWithoutCleanup(OperationContext* opCtx,
                                     const CollectionPtr& collection,
                                     bool isResumable) = 0;

    /**
     * Returns true if this build block supports background writes while building an index. This is
     * true for the kHybrid method.
     */
    virtual bool isBackgroundBuilding() const = 0;

    virtual void setIndexBuildMethod(IndexBuildMethod indexBuildMethod) = 0;
};
}  // namespace mongo
