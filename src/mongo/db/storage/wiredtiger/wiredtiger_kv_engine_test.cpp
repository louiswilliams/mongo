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

#include "mongo/platform/basic.h"

#include "mongo/db/storage/kv/kv_engine_test_harness.h"

#include <boost/filesystem.hpp>
#include <boost/filesystem/path.hpp>

#include "mongo/base/init.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/repl/repl_settings.h"
#include "mongo/db/repl/replication_coordinator_mock.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_record_store.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/temp_dir.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/clock_source_mock.h"

namespace mongo {
namespace {

class WiredTigerKVHarnessHelper : public KVHarnessHelper {
public:
    WiredTigerKVHarnessHelper(bool forRepair = false)
        : _dbpath("wt-kv-harness"), _forRepair(forRepair) {
        _engine.reset(makeEngine());
        repl::ReplicationCoordinator::set(
            getGlobalServiceContext(),
            std::unique_ptr<repl::ReplicationCoordinator>(new repl::ReplicationCoordinatorMock(
                getGlobalServiceContext(), repl::ReplSettings())));
    }

    virtual ~WiredTigerKVHarnessHelper() {
        _engine.reset(nullptr);
    }

    virtual KVEngine* restartEngine() override {
        _engine.reset(nullptr);
        _engine.reset(makeEngine());
        return _engine.get();
    }

    virtual KVEngine* getEngine() override {
        return _engine.get();
    }

    virtual WiredTigerKVEngine* getWiredTigerKVEngine() {
        return _engine.get();
    }

private:
    WiredTigerKVEngine* makeEngine() {
        return new WiredTigerKVEngine(kWiredTigerEngineName,
                                      _dbpath.path(),
                                      _cs.get(),
                                      "",
                                      1,
                                      false,
                                      false,
                                      _forRepair,
                                      false);
    }

    const std::unique_ptr<ClockSource> _cs = stdx::make_unique<ClockSourceMock>();
    unittest::TempDir _dbpath;
    std::unique_ptr<WiredTigerKVEngine> _engine;
    bool _forRepair;
};

class WiredTigerKVEngineTest : public unittest::Test {
public:
    void setUp() override {
        setGlobalServiceContext(ServiceContext::make());
        Client::initThread(getThreadName());

        _helper = makeHelper();
        _engine = _helper->getWiredTigerKVEngine();
    }

    void tearDown() override {
        _helper.reset(nullptr);
        Client::destroy();
        setGlobalServiceContext({});
    }

    std::unique_ptr<OperationContext> makeOperationContext() {
        return std::make_unique<OperationContextNoop>(_engine->newRecoveryUnit());
    }

protected:
    virtual std::unique_ptr<WiredTigerKVHarnessHelper> makeHelper() {
        return std::make_unique<WiredTigerKVHarnessHelper>();
    }

    std::unique_ptr<WiredTigerKVHarnessHelper> _helper;
    WiredTigerKVEngine* _engine;
};

class WiredTigerKVEngineRepairTest : public WiredTigerKVEngineTest {
    virtual std::unique_ptr<WiredTigerKVHarnessHelper> makeHelper() override {
        return std::make_unique<WiredTigerKVHarnessHelper>(true /* repair */);
    }
};

TEST_F(WiredTigerKVEngineRepairTest, OrphanedDataFilesCanBeRecovered) {

    std::string ns = "a.b";
    std::string ident = "collection-1234";
    std::string record = "abcd";
    CollectionOptions options;

    RecordId loc;
    {
        auto opCtxPtr = makeOperationContext();
        std::unique_ptr<RecordStore> rs;
        ASSERT_OK(_engine->createRecordStore(opCtxPtr.get(), ns, ident, options));
        rs = _engine->getRecordStore(opCtxPtr.get(), ns, ident, options);
        ASSERT(rs);

        WriteUnitOfWork uow(opCtxPtr.get());
        StatusWith<RecordId> res =
            rs->insertRecord(opCtxPtr.get(), record.c_str(), record.length() + 1, Timestamp());
        ASSERT_OK(res.getStatus());
        loc = res.getValue();
        uow.commit();

        // Force a checkpoint to guarantee the data is written to disk. This will also prevent any
        // EBUSY errors when dropping the ident later on.
        _engine->flushAllFiles(opCtxPtr.get(), true /* sync */);
    }

    // Copy the data file to a temporary location so the ident can be dropped.
    const boost::optional<boost::filesystem::path> dataFilePath =
        _engine->getDataFilePathForIdent(ident);
    ASSERT(dataFilePath);

    ASSERT(boost::filesystem::exists(*dataFilePath));

    const boost::filesystem::path tmpFile{dataFilePath->string() + ".tmp"};
    ASSERT(!boost::filesystem::exists(tmpFile));

    // Copy instead of move because the file may still be open by the storage engine.
    boost::system::error_code err;
    boost::filesystem::copy_file(*dataFilePath, tmpFile, err);
    ASSERT(!err) << err.message();

    {
        auto opCtxPtr = makeOperationContext();
        ASSERT_OK(_engine->dropIdent(opCtxPtr.get(), ident));
    }

    // The file should be deleted for good.
    ASSERT(!boost::filesystem::exists(*dataFilePath, err));

    // The data file is moved back in place so that it becomes an "orphan" of the storage
    // engine and the restoration process can be tested.
    boost::filesystem::rename(tmpFile, *dataFilePath, err);
    ASSERT(!err) << err.message();

    {
        auto opCtxPtr = makeOperationContext();
        ASSERT_OK(_engine->recoverOrphanedIdent(opCtxPtr.get(), ns, ident, options));

        // The original record should still be around.
        std::unique_ptr<RecordStore> rs;
        rs = _engine->getRecordStore(opCtxPtr.get(), ns, ident, options);
        ASSERT_EQUALS(record, rs->dataFor(opCtxPtr.get(), loc).data());
    }
}

TEST_F(WiredTigerKVEngineRepairTest, UnrecoverableOrphanedDataFilesFailGracefully) {

    std::string ns = "a.b";
    std::string ident = "collection-1234";
    std::string record = "abcd";
    CollectionOptions options;

    {
        auto opCtxPtr = makeOperationContext();
        std::unique_ptr<RecordStore> rs;
        ASSERT_OK(_engine->createRecordStore(opCtxPtr.get(), ns, ident, options));
        rs = _engine->getRecordStore(opCtxPtr.get(), ns, ident, options);
        ASSERT(rs);

        WriteUnitOfWork uow(opCtxPtr.get());
        StatusWith<RecordId> res =
            rs->insertRecord(opCtxPtr.get(), record.c_str(), record.length() + 1, Timestamp());
        ASSERT_OK(res.getStatus());
        uow.commit();

        // Force a checkpoint to guarantee the data is written to disk. This will also prevent any
        // EBUSY errors when dropping the ident later on.
        _engine->flushAllFiles(opCtxPtr.get(), true /* sync */);
    }

    const boost::optional<boost::filesystem::path> dataFilePath =
        _engine->getDataFilePathForIdent(ident);
    ASSERT(dataFilePath);

    ASSERT(boost::filesystem::exists(*dataFilePath));

    {
        auto opCtxPtr = makeOperationContext();
        ASSERT_OK(_engine->dropIdent(opCtxPtr.get(), ident));
    }

    ASSERT(!boost::filesystem::exists(*dataFilePath));

    // Create an empty data file. The subsequent call to recreate the collection will fail because
    // it is unsalvageable.
    boost::filesystem::ofstream fileStream(*dataFilePath);
    fileStream << "";
    fileStream.close();

    ASSERT(boost::filesystem::exists(*dataFilePath));

    // This should fail gracefully and not cause any crashing.
    {
        auto opCtxPtr = makeOperationContext();
        ASSERT_NOT_OK(_engine->recoverOrphanedIdent(opCtxPtr.get(), ns, ident, options));
    }
}

std::unique_ptr<KVHarnessHelper> makeHelper() {
    return stdx::make_unique<WiredTigerKVHarnessHelper>();
}

MONGO_INITIALIZER(RegisterKVHarnessFactory)(InitializerContext*) {
    KVHarnessHelper::registerFactory(makeHelper);
    return Status::OK();
}

}  // namespace
}  // namespace mongo
