/**
 *    Copyright (C) 2021-present MongoDB, Inc.
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

#include "mongo/platform/basic.h"

#include <type_traits>

#include "mongo/bson/bson_column.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/json.h"
#include "mongo/logv2/log_debug.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/hex.h"

namespace mongo {
namespace {

class BSONColumnExample : public unittest::Test {
public:
    void setUp() override {
        BSONBinData bin(exampleData, sizeof(exampleData), BinDataType::Column);
        BSONObjBuilder bob;
        bob.append(exampleFieldName, bin);
        exampleObj = bob.obj();
        exampleCol = BSONColumn(exampleObj[exampleFieldName]);
    }
    BSONObj exampleObj;
    BSONColumn exampleCol;

protected:
    // This example encodes a typical column of metrics, which looks as follows:
    // { 0: 72.0, 1: 72.0, ..., 100: 72.0,  (Repeating values)
    //   101: 72.5, 102: 73.0, 103: 73.5,   (Applying delta)
    //   106: 73.5 }                        (Skipping values)
    unsigned char exampleData[18] = {0x01,  // Number Double 72.0
                                     0x00,
                                     0x00,
                                     0x00,
                                     0x00,
                                     0x00,
                                     0x00,
                                     0x00,
                                     0x52,
                                     0x40,
                                     0x86,  // Repeat 99 times
                                     0x43,
                                     0x81,
                                     0x6b,  // set delta to 0x2'0000'0000'0000, output 100: 72.5
                                     0x32,  // Apply delta twice: output 101: 73.0, 102: 73.5
                                     0x22,
                                     0x41,
                                     0x00};
    const StringData exampleFieldName = "col"_sd;
};

TEST_F(BSONColumnExample, Basic) {
    BSONColumn col;
    ASSERT(col.isEmpty());
    ASSERT_EQ(col.nFields(), 0);
    ASSERT_EQ(col.objsize(), 1);  // EOO is 1 byte
    ASSERT(col.begin() == col.end());

    ASSERT_FALSE(exampleCol.isEmpty());
    ASSERT_EQ(exampleCol.objsize(),
              exampleFieldName.size() + 1 /*NUL*/ + 1 /*BSONType*/ + 4 /* int32 size */ +
                  1 /*BinDataType*/ + sizeof(exampleData));
    ASSERT_FALSE(exampleCol.begin() == exampleCol.end());
}

TEST_F(BSONColumnExample, SimpleIteration) {
    BSONObjBuilder bob;
    for (auto it = exampleCol.begin(); it != exampleCol.end(); ++it)
        bob.appendAs(*it, fmt::to_string(it.index()));
    BSONObj obj = bob.done();
    logd("expanded: {}, BSONColumn size: {}, BSONObj size: {}",
         obj,
         exampleCol.objsize(),
         obj.objsize());
    ASSERT_EQ(exampleCol.nFields(), obj.nFields());
}

TEST_F(BSONColumnExample, SimpleLookup) {
    auto elem = exampleCol[1];
    ASSERT_EQ(elem.type(), BSONType::NumberDouble);
    ASSERT_EQ(elem.numberDouble(), 72.0);

    elem = exampleCol[2];
    ASSERT_EQ(elem.type(), BSONType::NumberDouble);
    ASSERT_EQ(elem.numberDouble(), 72.0);

    elem = exampleCol[100];
    ASSERT_EQ(elem.type(), BSONType::NumberDouble);
    ASSERT_EQ(elem.numberDouble(), 72.5);

    elem = exampleCol[103];  // Missing field
    ASSERT_EQ(elem.type(), BSONType::EOO);
}

TEST_F(BSONColumnExample, SimpleBuild) {
    BSONObjBuilder bob;
    BufBuilder buf;
    {
        BSONColumn::Builder col(buf, "col");
        for (auto it = exampleCol.begin(); it != exampleCol.end(); ++it) {
            bob.appendAs(*it, fmt::to_string(it.index()));
            col.append(it.index(), *it);
        }
    }
    BSONObj example = bob.obj();
    logd("example = {}", example);

    BSONElement elem(buf.buf());
    ASSERT(elem.type() == BinData && elem.binDataType() == Column);
    int len;
    const char* data;
    data = elem.binDataClean(len);
    logd("Got a bindata of length {}", len);
    logd("disassemble: {}", BSONColumn::Instruction::disassemble(data, len));
    logd("hex: {}", hexdump(data, len));
    logd("disassemble example: {}",
         BSONColumn::Instruction::disassemble(reinterpret_cast<char*>(exampleData),
                                              sizeof(exampleData)));
    logd("hex: {}", hexdump(reinterpret_cast<char*>(exampleData), sizeof(exampleData)));
}

}  // namespace
}  // namespace mongo
