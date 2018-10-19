/**
 *    Copyright (C) 2018 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kTransaction

#include "mongo/platform/basic.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/catalog_raii.h"
#include "mongo/db/commands.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/find_common.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {

class ParallelScanCommand : public BasicCommand {
public:
    ParallelScanCommand() : BasicCommand("parallelScan") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    virtual bool adminOnly() const {
        return false;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    std::string help() const override {
        return "Scan a collection in parallel";
    }

    Status checkAuthForOperation(OperationContext* opCtx,
                                 const std::string& dbname,
                                 const BSONObj& cmdObj) const override {
        return Status::OK();
    }

    bool run(OperationContext* opCtx,
             const std::string& dbName,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {

        NamespaceString nss(CommandHelpers::parseNsFromCommand(dbName, cmdObj));

        AutoGetCollection autoColl(opCtx, nss, MODE_IS);

        boost::intrusive_ptr<ExpressionContext> expCtx(new ExpressionContext(opCtx, nullptr));
        BSONObj matchBSON = cmdObj["filter"].Obj();
        auto swMatchExpr = MatchExpressionParser::parse(matchBSON, expCtx);
        uassertStatusOK(swMatchExpr.getStatus());

        auto matchExpr = std::move(swMatchExpr.getValue());

        SharedScanCursor sharedCursor(autoColl.getCollection()->getRecordStore(), matchExpr.get());
        sharedCursor.init();

        BufBuilder bb(FindCommon::kInitReplyBufferSize);
        bb.skip(sizeof(QueryResult::Value));

        auto batch = sharedCursor.nextBatch();
        while (!batch.empty()) {
            LOG(0) << "Got documents in batch: " << batch.size();
            batch = sharedCursor.nextBatch();
        }


        return true;
    }

} commitTxn;

}  // namespace
}  // namespace mongo
