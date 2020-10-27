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

#define MONGO_LOGV2_DEFAULT_COMPONENT ::mongo::logv2::LogComponent::kIndex

#include "mongo/platform/basic.h"

#include "mongo/db/catalog/parallel_index_builder.h"

#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/logv2/log.h"

namespace mongo {
const StringData kIndexBuilderName = "ParallelIndexBuilder"_sd;

void ParallelIndexExecutorHolder::startup() {
    ThreadPool::Options options;
    options.poolName = kIndexBuilderName.toString();
    options.minThreads = 0;
    options.maxThreads = ThreadPool::Options::kUnlimited;
    options.onCreateThread = [](const std::string& threadName) {
        Client::initThread(threadName.c_str());
    };

    _threadPool = std::make_unique<ThreadPool>(std::move(options));
    _threadPool->startup();
}

ParallelIndexBuilder::~ParallelIndexBuilder() {}

StatusWith<std::vector<BSONObj>> ParallelIndexBuilder::init(OperationContext* opCtx,
                                                            CollectionWriter& collection,
                                                            const BSONObj& spec,
                                                            OnInitFn onInit) {
    try {
        WriteUnitOfWork wunit(opCtx);

        _maxMemoryUsageBytes = 400 * 1024 * 1024;

        // Initializing individual index build blocks below performs un-timestamped writes to the
        // durable catalog. It's possible for the onInit function to set multiple timestamps
        // depending on the index build codepath taken. Once to persist the index build entry in the
        // 'config.system.indexBuilds' collection and another time to log the operation using
        // onStartIndexBuild(). It's imperative that the durable catalog writes are timestamped at
        // the same time as onStartIndexBuild() is to avoid rollback issues.
        std::vector<BSONObj> unused;
        Status status = onInit(unused);
        if (!status.isOK()) {
            return status;
        }

        StatusWith<BSONObj> statusWithInfo =
            collection->getIndexCatalog()->prepareSpecForCreate(opCtx, spec, boost::none);
        if (!statusWithInfo.isOK()) {
            return statusWithInfo.getStatus();
        }
        BSONObj info = statusWithInfo.getValue();

        _buildBlock = std::make_unique<IndexBuildBlock>(
            collection->ns(), info, IndexBuildMethod::kHybrid, _buildUUID);

        status = _buildBlock->init(opCtx, collection.getWritableCollection());
        if (!status.isOK())
            return status;

        auto indexCatalogEntry = _buildBlock->getEntry(opCtx, collection.getWritableCollection());
        auto accessMethod = indexCatalogEntry->accessMethod();
        _accessMethod = accessMethod;

        LOGV2(0, "building index with parallelism", "parallelism"_attr = _parallelism);

        // Create as many partial states up to the maximum.
        for (auto i = 0; i < _parallelism; i++) {
            PartialState state;
            state.accessMethod = accessMethod;
            state.bulkBuilder =
                accessMethod->initiateBulk(_maxMemoryUsageBytes / _parallelism, boost::none);
            _partialStates.push_back(std::move(state));
        }

        const IndexDescriptor* descriptor = indexCatalogEntry->descriptor();

        collection->getIndexCatalog()->prepareInsertDeleteOptions(
            opCtx, collection->ns(), descriptor, &_options);

        // Index builds always relax constraints and check for violations at commit-time.
        _options.getKeysMode = IndexAccessMethod::GetKeysMode::kRelaxConstraints;
        _options.dupsAllowed = true;
        _options.fromIndexBuilder = true;

        _bulkLoader = accessMethod->makeBulkBuilder(opCtx, _options.dupsAllowed);

        LOGV2(99999,
              "Index build: starting",
              logAttrs(collection->ns()),
              "buildUUID"_attr = _buildUUID,
              "properties"_attr = *descriptor,
              "maxMemoryUsageMB"_attr = _maxMemoryUsageBytes / 1024 / 1024);

        opCtx->recoveryUnit()->onCommit(
            [ns = collection->ns(), uuid = collection->uuid(), this](auto commitTs) {
                if (!_buildUUID) {
                    return;
                }

                LOGV2(20346,
                      "Index build: initialized",
                      "buildUUID"_attr = _buildUUID,
                      logAttrs(ns),
                      "collectionUUID"_attr = uuid,
                      "initializationTimestamp"_attr = commitTs);
            });

        wunit.commit();
        return std::vector{info};
    } catch (const WriteConflictException&) {
        // Avoid converting WCE to Status.
        throw;
    } catch (...) {
        return exceptionToStatus().withContext(
            str::stream() << "Caught exception during index builder (" << _buildUUID
                          << ") initialization on namespace" << collection->ns() << " ("
                          << collection->uuid() << "). " << spec);
    }
}

StatusWith<std::vector<BSONObj>> ParallelIndexBuilder::init(
    OperationContext* opCtx,
    CollectionWriter& collection,
    const std::vector<BSONObj>& indexSpecs,
    OnInitFn onInit,
    const boost::optional<ResumeIndexInfo>& resumeInfo) {
    invariant(!resumeInfo);
    invariant(indexSpecs.size() == 1);
    return init(opCtx, collection, indexSpecs[0], onInit);
}

namespace {
void generateKeysForRange(OperationContext* opCtx,
                          NamespaceStringOrUUID nssOrUUID,
                          InsertDeleteOptions options,
                          ParallelIndexBuilder::PartialState* state,
                          ParallelIndexBuilder::Range range) {
    AutoGetCollection coll(opCtx, nssOrUUID, MODE_IX);
    auto cursor = coll.getCollection()->getRecordStore()->getCursor(opCtx);
    auto record = cursor->seekExact(range.min);
    while (record && record->id <= range.max) {
        uassertStatusOK(
            state->bulkBuilder->insert(opCtx, record->data.toBson(), record->id, options));
        record = cursor->next();
    }
}
}  // namespace

void ParallelIndexBuilder::_scheduleBatch(OperationContext* opCtx,
                                          NamespaceStringOrUUID nssOrUUID,
                                          Range range) {
    PartialState state = [&] {
        stdx::unique_lock<Latch> lk(_partialStateMutex);
        if (_partialStates.empty()) {
            LOGV2(0, "Waiting for more partial states");
            opCtx->waitForConditionOrInterrupt(
                _statesEmptyCond, lk, [&] { return !_partialStates.empty(); });
        }
        auto p = std::move(_partialStates.front());
        _partialStates.pop_front();
        return p;
    }();

    {
        stdx::unique_lock<Latch> lk(_outstandingTasksMutex);
        _outstandingTasks++;
    };

    _threadPool->schedule([this, nssOrUUID, options = _options, state = std::move(state), range](
                              auto status) mutable {
        auto uniqueOpCtx = Client::getCurrent()->makeOperationContext();
        auto opCtx = uniqueOpCtx.get();

        // Can't throw
        generateKeysForRange(opCtx, nssOrUUID, options, &state, range);

        LOGV2(0, "Completed range", "min"_attr = range.min, "max"_attr = range.max);

        {
            stdx::unique_lock<Latch> lk(_partialStateMutex);
            _partialStates.push_back(std::move(state));
            _statesEmptyCond.notify_all();
        }

        {
            stdx::unique_lock<Latch> lk(_outstandingTasksMutex);
            _outstandingTasks--;
            if (_outstandingTasks == 0) {
                _noOutstandingTasksCond.notify_all();
            }
        };
    });
};

Status ParallelIndexBuilder::_scheduleBatchesByScanning(OperationContext* opCtx,
                                                        const CollectionPtr& collection) {

    NamespaceStringOrUUID nssOrUUID(collection->ns().db().toString(), collection->uuid());

    PlanYieldPolicy::YieldPolicy yieldPolicy = PlanYieldPolicy::YieldPolicy::YIELD_AUTO;
    auto exec = collection->makePlanExecutor(
        opCtx, collection, yieldPolicy, Collection::ScanDirection::kForward);

    unsigned long long n = 0;

    RecordId batchMin;
    int currentBatchSize = 0;

    BSONObj objToIndex;
    RecordId loc;
    PlanExecutor::ExecState state;
    while (PlanExecutor::ADVANCED == (state = exec->getNext(&objToIndex, &loc))) {
        opCtx->checkForInterrupt();

        if (batchMin.isNull()) {
            batchMin = loc;
        }

        currentBatchSize++;
        n++;
        if (currentBatchSize < _maxBatchSize) {
            continue;
        }

        _scheduleBatch(opCtx, nssOrUUID, Range{batchMin, loc});

        batchMin = RecordId();
        currentBatchSize = 0;
    }

    // Apply final, non-full batch.
    if (!batchMin.isNull() && batchMin != loc) {
        _scheduleBatch(opCtx, nssOrUUID, Range{batchMin, loc});
    }

    LOGV2(0,
          "Index build: done generating batches by scanning",
          "buildUUID"_attr = _buildUUID,
          "records"_attr = n);
    return Status::OK();
}

Status ParallelIndexBuilder::_scheduleBatchesBySampling(OperationContext* opCtx,
                                                        const CollectionPtr& collection) {
    NamespaceStringOrUUID nssOrUUID(collection->ns().db().toString(), collection->uuid());

    RecordId firstId = [&] {
        auto cursor = collection->getRecordStore()->getCursor(opCtx, /* forward */ true);
        auto record = cursor->next();
        if (!record) {
            return RecordId();
        }
        return record->id;
    }();
    if (firstId.isNull()) {
        return Status::OK();
    }

    RecordId lastId = [&] {
        auto cursor = collection->getRecordStore()->getCursor(opCtx, /* forward */ false);
        auto record = cursor->next();
        if (!record) {
            return RecordId();
        }
        return record->id;
    }();

    auto numRecords = collection->getRecordStore()->numRecords(opCtx);
    size_t numBatches = numRecords / _maxBatchSize;

    LOGV2(0, "Taking samples", "samples"_attr = numBatches);

    std::vector<RecordId> samples;
    samples.reserve(numBatches);
    samples.push_back(firstId);
    samples.push_back(lastId);

    auto randCursor = collection->getRecordStore()->getRandomCursor(opCtx);
    while (samples.size() < numBatches) {
        auto record = randCursor->next();
        invariant(record);

        samples.push_back(record->id);
    }

    std::sort(samples.begin(), samples.end());
    RecordId prevId;
    for (auto& sample : samples) {
        if (prevId.isNull()) {
            prevId = sample;
            continue;
        }

        // Ensure there is no overlapping of ranges. The min range must exist, but the max does not.
        auto max = RecordId(sample.repr() - 1);

        LOGV2(0, "scheduling batch", "min"_attr = prevId, "max"_attr = max);
        _scheduleBatch(opCtx, nssOrUUID, Range{prevId, max});
        prevId = sample;
    }
    return Status::OK();
}

Status ParallelIndexBuilder::insertAllDocumentsInCollection(
    OperationContext* opCtx,
    const CollectionPtr& collection,
    boost::optional<RecordId> resumeAfterRecordId) {
    invariant(!resumeAfterRecordId);
    invariant(isBackgroundBuilding());

    // Hint to the storage engine that this collection scan should not keep data in the cache.
    // opCtx->recoveryUnit()->setReadOnce(true);


    Timer t;
    bool sample = true;
    try {

        auto status = [&] {
            if (sample) {
                return _scheduleBatchesBySampling(opCtx, collection);
            } else {
                return _scheduleBatchesByScanning(opCtx, collection);
            }
        }();
        if (!status.isOK()) {
            return status;
        }

        // Don't hold on to a snapshot while waiting.
        opCtx->recoveryUnit()->abandonSnapshot();
        {
            stdx::unique_lock<Latch> lk(_outstandingTasksMutex);
            LOGV2(0, "Waiting for all batches to finish");
            opCtx->waitForConditionOrInterrupt(
                _noOutstandingTasksCond, lk, [&] { return _outstandingTasks == 0; });
        }

        // Finish sorting.
        std::vector<IndexAccessMethod::BulkBuilder*> builders;
        builders.reserve(_partialStates.size());
        for (auto& partial : _partialStates) {
            builders.emplace_back(partial.bulkBuilder.get());
        }

        LOGV2(0, "Merging results");

        // Merge
        auto mergeIterator = _accessMethod->makeMergedIterator(builders, _maxMemoryUsageBytes);
        while (mergeIterator->more()) {
            // TODO: Is WUOW necessary?
            WriteUnitOfWork wunit(opCtx);

            auto data = mergeIterator->next();
            auto status = _bulkLoader->addKey(data.first);
            if (!status.isOK()) {
                return status;
            }
            wunit.commit();
        }

        WriteUnitOfWork wunit(opCtx);
        _bulkLoader->commit(true);
        wunit.commit();

    } catch (DBException& ex) {
        auto readSource = opCtx->recoveryUnit()->getTimestampReadSource();
        LOGV2(4984704,
              "Index build: collection scan stopped",
              "buildUUID"_attr = _buildUUID,
              "duration"_attr = duration_cast<Milliseconds>(Seconds(t.seconds())),
              "readSource"_attr = RecoveryUnit::toString(readSource),
              "error"_attr = ex);
        return ex.toStatus();
    }
    return Status::OK();
}

Status ParallelIndexBuilder::drainBackgroundWrites(
    OperationContext* opCtx,
    RecoveryUnit::ReadSource readSource,
    IndexBuildInterceptor::DrainYieldPolicy drainYieldPolicy) {
    return Status::OK();
}


Status ParallelIndexBuilder::retrySkippedRecords(OperationContext* opCtx,
                                                 const CollectionPtr& collection) {
    return Status::OK();
}
Status ParallelIndexBuilder::checkConstraints(OperationContext* opCtx,
                                              const CollectionPtr& collection) {
    return Status::OK();
}

Status ParallelIndexBuilder::commit(OperationContext* opCtx,
                                    Collection* collection,
                                    OnCreateEachFn onCreateEach,
                                    OnCommitFn onCommit) {
    _buildBlock->success(opCtx, collection);

    opCtx->recoveryUnit()->onCommit([opCtx, this](auto commitTs) {
        _buildBlock->finalizeTemporaryTables(opCtx,
                                             TemporaryRecordStore::FinalizationAction::kDelete);
    });

    onCommit();

    return Status::OK();
}

void ParallelIndexBuilder::abortIndexBuild(OperationContext* opCtx,
                                           CollectionWriter& collection,
                                           OnCleanUpFn onCleanUp) noexcept {}

void ParallelIndexBuilder::abortWithoutCleanup(OperationContext* opCtx,
                                               const CollectionPtr& collection,
                                               bool isResumable) {}

}  // namespace mongo