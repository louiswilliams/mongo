/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
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

#include "mongo/platform/basic.h"

#include "mongo/base/disallow_copying.h"
#include "mongo/db/service_context.h"

namespace mongo {

/**
 * A StorageEngineRepairManager is responsible for managing the state of the repair process. It
 * handles state transitions so that failed repairs are recoverable and so that replica set
 * corruption is not possible.
 * */
class StorageEngineRepairManager {
public:
    MONGO_DISALLOW_COPYING(StorageEngineRepairManager);

    StorageEngineRepairManager() : _repairState(RepairState::kStable) {}

    virtual ~StorageEngineRepairManager() {}

    static StorageEngineRepairManager* get(ServiceContext* service);
    static void set(ServiceContext* service,
                    std::unique_ptr<StorageEngineRepairManager> repairManager);

    enum class RepairState {
        /**
         * No data has been modified. If the process were to exit, the server may be started again
         * safely.
         */
        kStable,
        /**
         * Data is in the act of being repaired. Data may or may not have been modified by
         * repair, but if the process were to exit in this state, we do not know. The server should
         * require it not start normally without retrying a repair operation.
         */
        kIncomplete,
        /**
         * Repair has completed and modified data. This node will be unable to rejoin its original
         * replica set.
         */
        kIncompleteMetadataRepaired,
        kRepairedDataModified,
        kRepairedDataUnmodified,

    };

    void setIncomplete() {
        _setState(RepairState::kIncomplete);
    }

    void setMetadataRepaired() {
        _setState(RepairState::kIncompleteMetadataRepaired);
    }

    void setDataRepaired(bool modified) {
        _setState(modified ? RepairState::kRepairedDataModified
                           : RepairState::kRepairedDataUnmodified);
    }

    bool isIncomplete() const {
        return _repairState == RepairState::kIncomplete;
    }

    bool getDataModifiedByRepair() {
        return _repairState == RepairState::kRepairedDataModified;
    }

    bool isModified() const {
        return _repairState == RepairState::kRepairedDataUnmodified;
    }


private:
    void _setState(RepairState state) {
        switch (state) {
            case RepairState::kStable:
            case RepairState::kIncomplete:
            case RepairState::kIncompleteMetadataRepaired:
            case RepairState::kRepairedDataModified:
            case RepairState::kRepairedDataUnmodified:
            default:
                break;
        }
        _repairState = state;
    }

    RepairState _repairState;
};

}  // namespace mongo
