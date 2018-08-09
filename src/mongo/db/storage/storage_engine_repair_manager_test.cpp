/**
 *    Copyright (C) 2018 MongoDB Inc.
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

#include "mongo/platform/basic.h"

#include "boost/filesystem.hpp"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/service_context_d_test_fixture.h"
#include "mongo/db/storage/storage_engine_repair_manager.h"
#include "mongo/unittest/death_test.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

static const NamespaceString kConfigNss("local.system.replset");
static const std::string kRepairIncompleteFileName = "_repair_incomplete";

using boost::filesystem::path;

class RepairManagerTest : public ServiceContextMongoDTest {
public:
    RepairManagerTest() : ServiceContextMongoDTest("ephemeralForTest") {

        repl::ReplicationCoordinator::set(
            getServiceContext(),
            stdx::make_unique<repl::ReplicationCoordinatorMock>(getServiceContext()));
    }

    void assertRepairIncompleteOnTearDown() {
        _assertRepairIncompleteOnTearDown = true;
    }

    void assertReplConfigValid(OperationContext* opCtx, bool valid) {
        BSONObj replConfig;
        bool found = Helpers::getSingleton(opCtx, "local.system.replset", replConfig);
        if (valid) {
            ASSERT(!found || !replConfig.hasField("repaired"));
        } else {
            ASSERT(found);
            ASSERT(replConfig.hasField("repaired"));
        }
    }

    path repairFilePath() {
        return path(storageGlobalParams.dbpath) / path(kRepairIncompleteFileName);
    }

    StorageEngineRepairManager* reset() {
        StorageEngineRepairManager::set(
            getServiceContext(),
            std::make_unique<StorageEngineRepairManager>(storageGlobalParams.dbpath));
        return getRepairManager();
    }

    StorageEngineRepairManager* getRepairManager() {
        return StorageEngineRepairManager::get(getServiceContext());
    }

    void tearDown() {
        if (_assertRepairIncompleteOnTearDown) {
            ASSERT(getRepairManager()->isIncomplete());
        } else {
            ASSERT(!getRepairManager()->isIncomplete());
        }
    }

private:
    bool _assertRepairIncompleteOnTearDown = false;
};

TEST_F(RepairManagerTest, DataUnmodified) {
    auto repairManager = getRepairManager();

    auto repairFile = repairFilePath();
    ASSERT(!boost::filesystem::exists(repairFile));
    ASSERT(!repairManager->isIncomplete());

    repairManager->onRepairStarted();

    ASSERT(repairManager->isIncomplete());
    ASSERT(boost::filesystem::exists(repairFile));

    auto opCtx = cc().makeOperationContext();
    repairManager->onRepairDone(opCtx.get(), StorageEngineRepairManager::DataState::kUnmodified);
    ASSERT(!repairManager->isIncomplete());
    ASSERT(!boost::filesystem::exists(repairFile));

    ASSERT(repairManager->isDone());
    ASSERT(!repairManager->isDataModified());

    assertReplConfigValid(opCtx.get(), true);
}

TEST_F(RepairManagerTest, DataModified) {
    auto repairManager = getRepairManager();

    auto repairFile = repairFilePath();
    ASSERT(!boost::filesystem::exists(repairFile));
    ASSERT(!repairManager->isIncomplete());

    repairManager->onRepairStarted();

    ASSERT(repairManager->isIncomplete());
    ASSERT(boost::filesystem::exists(repairFile));

    auto opCtx = cc().makeOperationContext();
    Lock::GlobalWrite lock(opCtx.get());
    repairManager->onRepairDone(opCtx.get(), StorageEngineRepairManager::DataState::kModified);
    ASSERT(!repairManager->isIncomplete());
    ASSERT(!boost::filesystem::exists(repairFile));

    ASSERT(repairManager->isDone());
    ASSERT(repairManager->isDataModified());
    assertReplConfigValid(opCtx.get(), false);
}

TEST_F(RepairManagerTest, RepairIsIncompleteOnFailure) {
    auto repairManager = getRepairManager();

    auto repairFile = repairFilePath();
    ASSERT(!boost::filesystem::exists(repairFile));
    ASSERT(!repairManager->isIncomplete());

    repairManager->onRepairStarted();

    ASSERT(repairManager->isIncomplete());
    ASSERT(boost::filesystem::exists(repairFile));

    // Assert that a failure to call onRepairDone does not remove the failure file.
    assertRepairIncompleteOnTearDown();
}

TEST_F(RepairManagerTest, RepairIncompleteAfterRestart) {
    auto repairManager = getRepairManager();
    ASSERT(!repairManager->isIncomplete());
    repairManager->onRepairStarted();
    ASSERT(repairManager->isIncomplete());

    repairManager = reset();
    ASSERT(repairManager->isIncomplete());


    // Assert that a failure to call onRepairStarted does not create the failure file.
    assertRepairIncompleteOnTearDown();
}

TEST_F(RepairManagerTest, RepairCompleteAfterRestart) {
    auto repairManager = getRepairManager();
    ASSERT(!repairManager->isIncomplete());
    repairManager->onRepairStarted();
    ASSERT(repairManager->isIncomplete());

    auto opCtx = cc().makeOperationContext();
    Lock::GlobalWrite lock(opCtx.get());
    repairManager->onRepairDone(opCtx.get(), StorageEngineRepairManager::DataState::kModified);
    ASSERT(repairManager->isDone());

    repairManager = reset();
    ASSERT(!repairManager->isIncomplete());
    // Done is reservered for completed operations.
    ASSERT(!repairManager->isDone());
    assertReplConfigValid(opCtx.get(), false);
}

DEATH_TEST_F(RepairManagerTest, FailsWhenDoneCalledFirst, "Invariant failure") {
    auto repairManager = getRepairManager();
    ASSERT(!repairManager->isIncomplete());

    auto opCtx = cc().makeOperationContext();
    repairManager->onRepairDone(opCtx.get(), StorageEngineRepairManager::DataState::kUnmodified);
}

DEATH_TEST_F(RepairManagerTest, FailsWhenStartedCalledAfterDone, "Invariant failure") {
    auto repairManager = getRepairManager();
    ASSERT(!repairManager->isIncomplete());
    repairManager->onRepairStarted();
    ASSERT(repairManager->isIncomplete());

    auto opCtx = cc().makeOperationContext();
    repairManager->onRepairDone(opCtx.get(), StorageEngineRepairManager::DataState::kUnmodified);
    ASSERT(repairManager->isDone());
    assertReplConfigValid(opCtx.get(), true);

    repairManager->onRepairStarted();
}
}  // namespace
}  // namespace mongo
