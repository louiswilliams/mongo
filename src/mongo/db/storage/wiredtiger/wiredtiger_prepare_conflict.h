// wiredtiger_prepare_conflict.h
/**
 *    Copyright (C) 2018 MongoDB Inc.
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

#include "mongo/db/curop.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_recovery_unit.h"
#include "mongo/util/fail_point_service.h"

namespace mongo {

// When set, randomly returns a prepare conflict with a provided probability between 0 and 1 of the
// "chance" field in the data of the failpoint.
MONGO_FP_FORWARD_DECLARE(WTPrepareConflictForReads);

/**
 * Logs a message with the number of prepare conflict retry attempts.
 */
void wiredTigerPrepareConflictLog(int attempt);

/**
 * Runs the argument function f as many times as needed for f to return an error other than
 * WT_PREPARE_CONFLICT. Each time f returns WT_PREPARE_CONFLICT we wait until the current unit of
 * work commits or aborts, and then try f again. Imposes no upper limit on the number of times to
 * re-try f, so any required timeout behavior must be enforced within f.
 * The function f must return a WiredTiger error code.
 */
template <typename F>
int wiredTigerPrepareConflictRetry(OperationContext* opCtx, F&& f) {
    invariant(opCtx);

    auto recoveryUnit = WiredTigerRecoveryUnit::get(opCtx);
    int attempts = 0;
    while (true) {
        attempts++;
        int ret = WT_READ_CHECK(f());

        // Randomly return a prepare conflict with a provided "chance" probability between 0 and 1.
        MONGO_FAIL_POINT_BLOCK(WTPrepareConflictForReads, scopedFailPoint) {
            double chance = scopedFailPoint.getData()["chance"].numberDouble();
            PseudoRandom rand(Date_t::now().asInt64());
            if (rand.nextCanonicalDouble() < chance)
                ret = WT_PREPARE_CONFLICT;
        }

        if (ret != WT_PREPARE_CONFLICT)
            return ret;

        ++CurOp::get(opCtx)->debug().prepareReadConflicts;
        wiredTigerPrepareConflictLog(attempts);
        // Wait on the session cache to signal that a unit of work has been committed or aborted.
        recoveryUnit->getSessionCache()->waitUntilPreparedUnitOfWorkCommitsOrAborts(opCtx);
    }
}
}  // namespace mongo
