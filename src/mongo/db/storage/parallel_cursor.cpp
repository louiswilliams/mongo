/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/db/storage/parallel_cursor.h"

#include "mongo/db/catalog_raii.h"
#include "mongo/db/client.h"
#include "mongo/db/operation_context.h"
#include "mongo/util/log.h"

namespace mongo {

void SharedScanCursor::init() {

    auto scheduler = _recordStore->getSharedScanScheduler();

    stdx::unique_lock<stdx::mutex> lk(_completedMutex);
    for (auto&& range : scheduler->getRanges()) {
        _outstanding.emplace_back(new WorkUnit(this, _filter, range));

        auto workPtr = _outstanding.back().get();
        scheduler->schedule(workPtr);
    }
}

void SharedScanCursor::completeWorkUnit(WorkUnit* done) {

    stdx::unique_lock<stdx::mutex> lk(_completedMutex);

    std::unique_ptr<WorkUnit> removed;
    auto it = _outstanding.begin();
    while (it != _outstanding.end()) {
        if ((*it)->id == done->id) {
            removed = std::move(*it);
            it = _outstanding.erase(it);
            break;
        }
        ++it;
    }

    LOG(1) << "completed " << removed->id << " with " << removed->out.size() << " records";

    _completed.push(std::move(removed));
    _completedCond.notify_all();
}

std::vector<Record> SharedScanCursor::_getNextCompletedBatch(stdx::unique_lock<stdx::mutex>& lk) {
    while (_completed.empty()) {
        _completedCond.wait(lk);
    }

    // Iterate through all empty vectors
    auto front = std::move(_completed.front());
    _completed.pop();  // remove

    while (front->out.empty()) {
        if (_completed.empty())
            return {};

        front = std::move(_completed.front());
        _completed.pop();  // remove
    }

    return front->out;
}

boost::optional<Record> SharedScanCursor::next() {
    if (_lastBatch.empty()) {
        _lastBatch = nextBatch();
    }

    if (_lastBatch.empty()) {
        return boost::none;
    }

    auto obj = std::move(_lastBatch.back());
    _lastBatch.pop_back();
    return obj;
}

std::vector<Record> SharedScanCursor::nextBatch() {

    stdx::unique_lock<stdx::mutex> lk(_completedMutex);
    if (_completed.empty() && _outstanding.empty()) {
        return {};
    }

    // If the batch is empty, we will wait.
    auto batch = _getNextCompletedBatch(lk);
    while (batch.empty()) {
        // Give up when all outstanding jobs are returned.
        if (_outstanding.empty()) {
            return {};
        }
        batch = _getNextCompletedBatch(lk);
    }
    return batch;
}

SharedScanWorker::SharedScanWorker(
    SharedScanScheduler* scheduler, RecordStore* recordStore, int id, RecordId min, RecordId max)
    : BackgroundJob(false),
      _scheduler(scheduler),
      _recordStore(recordStore),
      _id(id),
      _minRecord(min),
      _maxRecord(max),
      _shutdown(false) {
    invariant(min > RecordId::min());
}

void SharedScanWorker::run() {
    while (!getGlobalServiceContext()->getStorageEngine()) {
        sleepmillis(10);
    }

    Client::initThread(name().c_str());
    ON_BLOCK_EXIT([] { Client::destroy(); });
    LOG(0) << "SharedScanWorker running on " << _recordStore->ns() << ". RecordId("
           << _minRecord.repr() << "," << _maxRecord.repr() << ")";


    auto opCtx = cc().makeOperationContext();

    auto inShutdown = _shutdown.load();
    while (!inShutdown) {
        inShutdown = !_scanRange(opCtx.get());
    }

    LOG(0) << "SharedScanWorker stopping on " << _recordStore->ns() << ". RecordId("
           << _minRecord.repr() << "," << _maxRecord.repr() << ")";
};


std::string SharedScanWorker::name() const {
    return str::stream() << "SharedScanWorker-" << _id;
};

bool SharedScanWorker::_scanRange(OperationContext* opCtx) {

    const NamespaceString nss(_recordStore->ns());

    // Get the queue of work units.
    std::vector<WorkUnit*> localQueue;
    {
        stdx::unique_lock<stdx::mutex> lk(_workMutex);
        while (_work.empty()) {
            if (_shutdown.load())
                return false;
            _workReadyCond.wait(lk);
        }

        std::swap(_work, localQueue);
    }

    AutoGetCollection autoColl(opCtx, nss, MODE_IS);

    std::unique_ptr<SeekableRecordCursor> cursor = _recordStore->getCursor(opCtx);

    auto record = cursor->seekExact(_minRecord);
    if (!record) {
        record = cursor->next();
    }
    int scanned = 0;

    while (record && record->id <= _maxRecord) {
        scanned++;

        const BSONObj& obj = record->data.toBson();

        for (auto workUnit : localQueue) {
            stdx::unique_lock<stdx::mutex> lk(workUnit->readyMutex);
            if (!workUnit->filter || workUnit->filter->matchesBSON(obj)) {
                workUnit->out.push_back(*record);  // copy
            }
        }
        record = cursor->next();
    }
    LOG(1) << "scanned " << scanned << " documents and processed " << localQueue.size()
           << " work items";

    // Notify cursors waiting for results.
    for (auto workUnit : localQueue) {
        // TODO: mark ready?
        _scheduler->markDone(workUnit);
    }
    return true;
}

void SharedScanWorker::enqueueWork(WorkUnit* work) {
    stdx::unique_lock<stdx::mutex> lk(_workMutex);
    _work.push_back(work);
    _workReadyCond.notify_all();
}

void SharedScanWorker::stop() {
    _shutdown.store(true);
    _workReadyCond.notify_all();
}

void SharedScanScheduler::markDone(WorkUnit* unit) {
    unit->cursor->completeWorkUnit(unit);
}

void SharedScanScheduler::stop() {
    stdx::unique_lock<stdx::mutex> lk(_workerMutex);
    for (auto& worker : _workers) {
        worker->stop();
    }

    for (auto& worker : _workers) {
        worker->wait();
    }
}

void SharedScanScheduler::start(int workers, RecordId start, RecordId end) {

    stdx::unique_lock<stdx::mutex> lk(_workerMutex);
    uint64_t partitionSize = (end.repr() - start.repr()) / workers;
    uint64_t partitionStart = start.repr();
    uint64_t partitionEnd = partitionStart + partitionSize;
    LOG(0) << "Creating " << workers << " workers with RecordId partition size: " << partitionSize;

    for (int i = 0; i < workers; i++) {
        auto worker = std::make_unique<SharedScanWorker>(
            this, _recordStore, i, RecordId(partitionStart), RecordId(partitionEnd));
        worker->go();
        _workers.emplace_back(std::move(worker));

        partitionStart = partitionEnd + 1;
        partitionEnd = partitionStart + partitionSize;
    }
}

uint64_t SharedScanScheduler::schedule(WorkUnit* work) {

    {
        stdx::unique_lock<stdx::mutex> lk(work->readyMutex);
        work->ready = false;
        work->id = _nextTaskId.fetchAndAdd(1);
    }

    LOG(1) << "scheduling " << work->id << " from " << work->inRange.first << " -> "
           << work->inRange.second;

    stdx::unique_lock<stdx::mutex> workerLk(_workerMutex);
    for (auto& worker : _workers) {
        if (work->inRange.first >= worker->getRange().first &&
            work->inRange.first < worker->getRange().second) {
            invariant(work->inRange.second <= worker->getRange().second);

            worker->enqueueWork(work);
            return work->id;
        }
    }

    return 0;
}

std::vector<std::pair<RecordId, RecordId>> SharedScanScheduler::getRanges() const {

    std::vector<std::pair<RecordId, RecordId>> _ranges;
    stdx::unique_lock<stdx::mutex> lk(_workerMutex);
    for (auto& worker : _workers) {
        _ranges.push_back(worker->getRange());
    }
    return _ranges;
}

void SharedScanScheduler::waitFor(uint64_t id) {}
}
