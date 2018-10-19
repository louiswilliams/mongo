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

#pragma once

#include <queue>
#include <vector>

#include <boost/optional.hpp>

#include "mongo/base/disallow_copying.h"
#include "mongo/bson/bsonobj.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/util/background.h"

namespace mongo {
class RecordStore;
class SharedScanCursor;
class SharedScanScheduler;
struct Record;


class WorkUnit {
public:
    WorkUnit(SharedScanCursor* cursor,
             const MatchExpression* filter,
             std::pair<RecordId, RecordId> range)
        : cursor(cursor), filter(filter), inRange(range) {}

    SharedScanCursor* cursor;
    const MatchExpression* filter;
    std::pair<RecordId, RecordId> inRange;

    int64_t id;


    std::vector<Record> out;
    std::pair<RecordId, RecordId> outRange;

    stdx::mutex readyMutex;
    stdx::condition_variable readyCond;
    bool ready;
};

class SharedScanCursor {
public:
    SharedScanCursor(const RecordStore* rs, const MatchExpression* filter)
        : _recordStore(rs), _filter(filter) {}

    ~SharedScanCursor() {
        invariant(_outstanding.empty());
    }

    void init();

    void completeWorkUnit(WorkUnit* unit);
    boost::optional<Record> next();

    std::vector<Record> nextBatch();

private:
    std::vector<Record> _getNextCompletedBatch(stdx::unique_lock<stdx::mutex>& withLock);

    const RecordStore* _recordStore;
    const MatchExpression* _filter;

    std::vector<Record> _lastBatch;

    stdx::mutex _completedMutex;
    stdx::condition_variable _completedCond;
    std::queue<std::unique_ptr<WorkUnit>> _completed;
    std::vector<std::unique_ptr<WorkUnit>> _outstanding;
};

class SharedScanWorker : public BackgroundJob {
public:
    SharedScanWorker(SharedScanScheduler* scheduler,
                     RecordStore* recordStore,
                     int id,
                     RecordId min,
                     RecordId max);

    virtual void run() override;

    void stop();

    void enqueueWork(WorkUnit* work);

    virtual std::string name() const override;

    std::pair<RecordId, RecordId> getRange() const {
        return {_minRecord, _maxRecord};
    }

private:
    void _scanRange(OperationContext* opCtx);

    SharedScanScheduler* _scheduler;
    RecordStore* _recordStore;
    int _id;
    RecordId _minRecord;
    RecordId _maxRecord;

    AtomicBool _shutdown;

    stdx::mutex _workMutex;
    stdx::condition_variable _workReadyCond;
    std::vector<WorkUnit*> _work;
};

class SharedScanScheduler {
public:
    SharedScanScheduler(RecordStore* recordStore) : _recordStore(recordStore) {}

    void start(int workers, RecordId start, RecordId end);
    void stop();

    uint64_t schedule(WorkUnit* work);
    void markDone(WorkUnit* work);

    void waitFor(uint64_t id);

    std::vector<std::pair<RecordId, RecordId>> getRanges() const;

private:
    RecordStore* _recordStore;

    mutable stdx::mutex _workerMutex;
    std::vector<std::unique_ptr<SharedScanWorker>> _workers;
    AtomicInt64 _nextTaskId;
};
}
