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

#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/multikey_paths.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/record_id.h"
#include "mongo/platform/atomic_word.h"

namespace mongo {

class BSONObj;
class IndexAccessMethod;
class OperationContext;

class IndexBuildInterceptor {
public:
    enum class Op { kInsert, kDelete };
    enum class ScanYield { kYieldAuto, kInterruptOnly };

    IndexBuildInterceptor() : _sideWritesNs(makeTempSideWritesNs()) {}
    IndexBuildInterceptor(NamespaceString sideWritesNs) : _sideWritesNs(sideWritesNs) {}

    static NamespaceString makeTempSideWritesNs();

    void ensureSideWritesCollectionExists(OperationContext* opCtx);
    void removeSideWritesCollection(OperationContext* opCtx);

    /**
     * Client writes that are concurrent with an index build will have their index updates written
     * to a temporary table. After the index table scan is complete, these updates will be applied
     * to the underlying index table.
     *
     * On success, `numKeysOut` if non-null will contain the number of keys added or removed.
     */
    Status sideWrite(OperationContext* opCtx,
                     IndexAccessMethod* indexAccessMethod,
                     const BSONObj* obj,
                     RecordId loc,
                     Op op,
                     int64_t* const numKeysOut);

    /**
     * Performs a resumable collection scan on the side writes collection, and either inserts or
     * removes each key from the underlying IndexAccessMethod. This will only insert as many records
     * as are visible in the current snapshot.
     *
     * This is resumable, so subsequent calls will start the collection scan at the record
     * immediately following the last inserted record from a previous call to drainWritesIntoIndex.
     *
     * If 'scanYield' is kYieldAuto, the collection scan will yield locks, and will not if it is set
     * to kInterruptOnly.
     *
     */
    Status drainWritesIntoIndex(OperationContext* opCtx,
                                IndexAccessMethod* indexAccessMethod,
                                const InsertDeleteOptions& options,
                                ScanYield scanYield);

    /**
     * Returns 'true' if there are no visible records remaining to be applied from the side writes
     * collection. Ensure that this returns 'true' when an index build is completed.
     */
    bool areAllWritesApplied(OperationContext* opCtx) const;

    /**
      * When an index builder wants to commit, use this to retrieve any recorded multikey paths
      * that were tracked during the build.
      */
    boost::optional<MultikeyPaths> getMultikeyPaths() const;

private:
    using SideWriteRecord = std::pair<RecordId, BSONObj>;

    RecordId _peekAtLastRecord(OperationContext* opCtx) const;

    Status _applyWrite(OperationContext* opCtx,
                       IndexAccessMethod* indexAccessMethod,
                       const BSONObj& doc,
                       const InsertDeleteOptions& options,
                       int64_t* const keysInserted,
                       int64_t* const keysDeleted);

    int64_t _numApplied{0};

    const NamespaceString _sideWritesNs;
    AtomicInt64 _sideWritesCounter{0};

    mutable stdx::mutex _multikeyPathMutex;
    boost::optional<MultikeyPaths> _multikeyPaths;
};


}  // namespace mongo
