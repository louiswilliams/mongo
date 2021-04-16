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
static bool kDebug = true;

class BSONColumnBasic : public unittest::Test {
public:
    static BSONObj expand(BSONColumn col) {
        BSONObjBuilder bob;
        for (auto it = col.begin(); it != col.end(); ++it)
            bob.appendAs(*it, fmt::to_string(it.index()));
        return bob.obj();
    }
    static BSONObj inspect(BSONColumn col) {
        BSONElement elem(col.objdata());
        BSONObj expanded = expand(col);
        int metrics = col.nFields();
        logd(
            "BSONColumn {}: Original = {:6.2f} bytes/metric, "
            "Compressed = {:6.2f} bytes/metric, "
            "Factor = {:6.2f}",
            elem.fieldName(),
            1.0 * expanded.objsize() / metrics,
            1.0 * col.objsize() / metrics,
            1.0 * expanded.objsize() / col.objsize());
        if (kDebug) {
            int len;
            const char* data;
            data = elem.binDataClean(len);
            logd("Got a bindata of length {}", len);
            logd("disassemble: {}", BSONColumn::Instruction::disassemble(data, len));
            logd("hex: {}", hexdump(data, len));
        }
        return expanded;
    }
};

class BSONColumnBuilder : public BSONColumnBasic {
public:
    BufBuilder buf;
    BSONColumn::Builder builder;
    BSONColumnBuilder() : builder(buf, "col") {}
    void check(BSONObj expected) {
        BSONColumn col = builder.done();
        inspect(col);
        auto colIt = col.begin();
        for (auto& elem : expected) {
            if (!elem.binaryEqualValues(*colIt)) {
                logd("values not binary equal: expected {}, but found {} {}",
                     elem,
                     colIt.index(),
                     *colIt);
                logd("expected: {}", expected);
                logd("found: {}", col);
                ASSERT(false);
            }
            ++colIt;
        }
    }
};

class BSONColumnExample : public BSONColumnBasic {
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

TEST_F(BSONColumnBasic, Empty) {
    BSONColumn col;
    ASSERT(col.isEmpty());
    ASSERT_EQ(col.nFields(), 0);
    ASSERT_EQ(col.objsize(), 1);  // EOO is 1 byte
    ASSERT(col.begin() == col.end());
}

TEST_F(BSONColumnBuilder, DeltaBuild) {
    BSONObj obj = BSON("0" << 0 << "1" << 1 << "2" << 2 << "3" << 2 << "4" << 4);
    for (auto& elem : obj)
        builder.append(elem);
    check(obj);
}

TEST_F(BSONColumnBuilder, WindSpeed) {
    BSONObj windspeed =
        BSON_ARRAY(6.0 << 6.5 << 4.3 << 9.2 << 11.4 << 7.8 << 12.1 << 11.4 << 5.8 << 5.1 << 3.4
                       << 7.6 << 7.4 << 7.6 << 7.4 << 6 << 5.6 << 5.4 << 6.7 << 2.5 << 5.4 << 6.3
                       << 10.5 << 5.4 << 6.5 << 4.0 << 2.7 << 3.4 << 7.6 << 8.9);
    for (auto speed : windspeed)
        builder.append(speed);
    check(windspeed);
}

TEST_F(BSONColumnBuilder, WindDirection) {
    BSONArray winddir = BSON_ARRAY(170.0 << 216.0 << 212.0 << 230.0 << 170.0 << 184.0);
    for (auto dir : winddir)
        builder.append(dir);

    check(winddir);
};

TEST_F(BSONColumnExample, ExampleBasic) {
    ASSERT_FALSE(exampleCol.isEmpty());
    ASSERT_EQ(exampleCol.objsize(),
              exampleFieldName.size() + 1 /*NUL*/ + 1 /*BSONType*/ + 4 /* int32 size */ +
                  1 /*BinDataType*/ + sizeof(exampleData));
    ASSERT_FALSE(exampleCol.begin() == exampleCol.end());
}

TEST_F(BSONColumnExample, ExampleIteration) {
    BSONObj obj = expand(exampleCol);
    if (kDebug)
        logd("expanded: {}, BSONColumn size: {}, BSONObj size: {}",
             obj,
             exampleCol.objsize(),
             obj.objsize());
    ASSERT_EQ(exampleCol.nFields(), obj.nFields());
}

TEST_F(BSONColumnExample, ExampleLookup) {
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

TEST_F(BSONColumnExample, ExampleBuild) {
    BSONObjBuilder bob;
    BufBuilder buf;
    BSONColumn::Builder builder(buf, "col");
    for (auto it = exampleCol.begin(); it != exampleCol.end(); ++it) {
        bob.appendAs(*it, fmt::to_string(it.index()));
        builder.append(it.index(), *it);
    }

    BSONObj example = bob.obj();
    BSONColumn col = builder.done();
    BSONElement elem(col.objdata());
    ASSERT_BSONELT_EQ(exampleObj[exampleFieldName], elem);
    BSONObj expanded = inspect(col);
    ASSERT_BSONOBJ_EQ(example, expanded);
}

TEST_F(BSONColumnExample, TSBSUsageNice) {
    if (1 + 1 == 2)
        return;  // disable for now, as the format's a changin
    StringData hex =
        "01000000000000804e40876b41875b976b876b836b31413241875b836b835b41876b835b483141876b875b4183"
        "5b42836b835b836b418b6b835b836b835b3141876b835b3141836b41876b836b875b836b41835b836b835b836b"
        "876b8b5b3141836b8b5b42835b41836b835b41936b5c6c5c936b8b5b8b6b875b876b3141836b41835b3141876b"
        "5c836b876b935b936b8b5b8b6b875b8b6b836b5c6c875b8b6b8b5b876b835b836b835b876b875b876b875b8b6b"
        "5c876b875b876b8b5b876b835b876b836b5c8b6b975b936b976bab5b936b9b6b975b935b826c815c816ca35b81"
        "6c9b5b9b6b975b9b6b9b5b976b935b6c8b5b6c8b5b936b935b31416c8b5b8b6b8b5b876b835b41836b835b836b"
        "41835b41876b8b5b876b41875b836b41835b443141836b835b875b835b3241875b836b835b836b3141875b4183"
        "5b3141876b835b3241836b43835b3141836b835b876b835b875b41836b835b836b31428b6b835b31433241875b"
        "3141a36ba35b9b6b835b5c8b6b875b836b875b6c975b976b975b976b975b9b6b9b5b9b6b9b5b976b975b936b8b"
        "5b836b8b5b876b875b8b6b935b976b875b876b875b836b8b5b42875b876b31413141875b876b41875b876b3242"
        "5c31413141876b875b876b976ba75ba76ba75b41816c875b6c975b9b6ba35ba36bab5bab6b876b835b876b836b"
        "3142875b835b836b314100"_sd;
    std::string bin = hexblob::decode(hex);

    auto obj = BSON("usage_nice" << BSONBinData(bin.data(), bin.length(), BinDataType::Column));
    auto elem = obj["usage_nice"];
    BSONColumn col(elem);
    inspect(col);
}

}  // namespace
}  // namespace mongo
