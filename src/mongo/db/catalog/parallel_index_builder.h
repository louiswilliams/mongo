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

#pragma once

#include <deque>

#include "mongo/db/catalog/index_build_block.h"
#include "mongo/db/catalog/index_builder_interface.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/platform/mutex.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/util/concurrency/thread_pool.h"

namespace mongo {

class ParallelIndexExecutorHolder {
public:
    void startup();
    ThreadPool* get() {
        return _threadPool.get();
    }

private:
    std::unique_ptr<ThreadPool> _threadPool;
};

class ParallelIndexBuilder : public IndexBuilderInterface {
public:
    ParallelIndexBuilder(ThreadPool* threadPool, int parallelism)
        : _threadPool(threadPool), _parallelism(parallelism){};

    ~ParallelIndexBuilder();

    struct Range {
        RecordId min;
        RecordId max;
    };

    struct PartialState {
        IndexAccessMethod* accessMethod;

        // Sorter
        std::unique_ptr<IndexAccessMethod::BulkBuilder> bulkBuilder;
        // Completed iterator
        std::unique_ptr<IndexAccessMethod::BulkBuilder::Sorter::Iterator> iterator;
    };

    void ignoreUniqueConstraint() override {}

    void setTwoPhaseBuildUUID(UUID indexBuildUUID) override {
        _buildUUID = indexBuildUUID;
    }

    StatusWith<std::vector<BSONObj>> init(
        OperationContext* opCtx,
        CollectionWriter& collection,
        const std::vector<BSONObj>& specs,
        OnInitFn onInit,
        const boost::optional<ResumeIndexInfo>& resumeInfo = boost::none) override;
    StatusWith<std::vector<BSONObj>> init(OperationContext* opCtx,
                                          CollectionWriter& collection,
                                          const BSONObj& spec,
                                          OnInitFn onInit) override;

    Status insertAllDocumentsInCollection(
        OperationContext* opCtx,
        const CollectionPtr& collection,
        boost::optional<RecordId> resumeAfterRecordId = boost::none) override;

    Status insertSingleDocumentForInitialSyncOrRecovery(OperationContext* opCtx,
                                                        const BSONObj& wholeDocument,
                                                        const RecordId& loc) override {
        return Status::OK();
    }

    Status dumpInsertsFromBulk(OperationContext* opCtx, const CollectionPtr& collection) override {
        return Status::OK();
    }

    Status dumpInsertsFromBulk(
        OperationContext* opCtx,
        const CollectionPtr& collection,
        const IndexAccessMethod::RecordIdHandlerFn& onDuplicateRecord) override {
        return Status::OK();
    }

    Status drainBackgroundWrites(OperationContext* opCtx,
                                 RecoveryUnit::ReadSource readSource,
                                 IndexBuildInterceptor::DrainYieldPolicy drainYieldPolicy) override;


    Status retrySkippedRecords(OperationContext* opCtx, const CollectionPtr& collection) override;

    Status checkConstraints(OperationContext* opCtx, const CollectionPtr& collection) override;

    Status commit(OperationContext* opCtx,
                  Collection* collection,
                  OnCreateEachFn onCreateEach,
                  OnCommitFn onCommit) override;

    void abortIndexBuild(OperationContext* opCtx,
                         CollectionWriter& collection,
                         OnCleanUpFn onCleanUp) noexcept override;

    void abortWithoutCleanup(OperationContext* opCtx,
                             const CollectionPtr& collection,
                             bool isResumable) override;

    bool isBackgroundBuilding() const override {
        return true;
    }

    void setIndexBuildMethod(IndexBuildMethod indexBuildMethod) override {}

private:
    template <typename Func>
    void _scheduleTask(OperationContext* opCtx, Func&& task);
    void _scheduleBatch(OperationContext* opCtx, NamespaceStringOrUUID nssOrUUID, Range range);

    void _waitForIdle(OperationContext* opCtx);

    PartialState _popState(OperationContext* opCtx);
    void _pushState(PartialState state);

    // Returns the highest RecordID on the collection.
    Status _scheduleBatchesBySampling(OperationContext* opCtx, const CollectionPtr& collection);

    boost::optional<UUID> _buildUUID;
    std::size_t _maxMemoryUsageBytes = 0;
    int _maxBatchSize = 1000;

    InsertDeleteOptions _options;
    IndexAccessMethod* _accessMethod;
    std::unique_ptr<IndexBuildBlock> _buildBlock;

    ThreadPool* _threadPool;
    int _parallelism;

    mutable Mutex _partialStateMutex = MONGO_MAKE_LATCH("ParallelIndexBuilder::_partialStateMutex");
    std::deque<PartialState> _partialStates;
    stdx::condition_variable _statesEmptyCond;

    mutable Mutex _outstandingTasksMutex =
        MONGO_MAKE_LATCH("ParallelIndexBuilder::_outstandingTasksMutex");
    int64_t _outstandingTasks = 0;
    stdx::condition_variable _noOutstandingTasksCond;
};

}  // namespace mongo
