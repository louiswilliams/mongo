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

#include <boost/functional/hash.hpp>
#include <boost/optional.hpp>
#include <climits>
#include <cstdint>
#include <ostream>

#include "mongo/bson/util/builder.h"
#include "mongo/logger/logstream_builder.h"
#include "mongo/util/bufreader.h"

namespace mongo {

/**
 * The key that uniquely identifies a Record in a Collection or RecordStore.
 */
class RecordId {
public:
    // This set of constants define the boundaries of the 'normal' and 'reserved' id ranges.
    static constexpr int64_t kNullRepr = 0;
    static constexpr int64_t kMinRepr = LLONG_MIN;
    static constexpr int64_t kMaxRepr = LLONG_MAX;
    static constexpr int64_t kMinReservedRepr = kMaxRepr - (1024 * 1024);

    /**
     * Enumerates all ids in the reserved range that have been allocated for a specific purpose.
     */
    enum class ReservedId : int64_t { kWildcardMultikeyMetadataId = kMinReservedRepr };

    /**
     * Constructs a Null RecordId.
     */
    RecordId() {}

    explicit RecordId(ReservedId repr) : RecordId(static_cast<int64_t>(repr)) {}
    explicit RecordId(int64_t repr) : RecordId(_fromRepr(repr)) {}

    explicit RecordId(const char* data, size_t len) : RecordId(std::string(data, len)) {}
    explicit RecordId(std::string data) : _data(data) {}

    // RecordId(const RecordId& other) : _data(other._data) {}
    // RecordId& operator=(const RecordId& other) {
    //_data = other._data;
    // return *this;
    //}
    /**
     * A RecordId that compares less than all ids that represent documents in a collection.
     */
    static RecordId min() {
        return RecordId(kMinRepr);
    }

    /**
     * A RecordId that compares greater than all ids that represent documents in a collection.
     */
    static RecordId max() {
        return RecordId(kMaxRepr);
    }

    /**
     * Returns the first record in the reserved id range at the top of the RecordId space.
     */
    static RecordId minReserved() {
        return RecordId(kMinReservedRepr);
    }

    size_t size() const {
        return _data.size();
    }


    bool isNull() const {
        if (size() == 0) {
            return true;
        }
        return size() == sizeof(uint64_t) ? repr() == 0 : false;
    }

    int64_t repr() const {
        invariant(_data.length() == sizeof(int64_t));
        return *static_cast<int64_t*>((void*)_data.c_str());
    }

    const std::string& data() const {
        return _data;
    }

    /**
     * Valid RecordIds are the only ones which may be used to represent Records. The range of valid
     * RecordIds includes both "normal" ids that refer to user data, and "reserved" ids that are
     * used internally. All RecordIds outside of the valid range are sentinel values.
     */
    bool isValid() const {
        return isNormal() || isReserved();
    }

    /**
     * Normal RecordIds are those which fall within the range used to represent normal user data,
     * excluding the reserved range at the top of the RecordId space.
     */
    bool isNormal() const {
        // "normal" means it can be represented as a 64-bit integer.
        if (size() != sizeof(uint64_t)) {
            return false;
        }
        return repr() > 0 && repr() < kMinReservedRepr;
    }

    /**
     * Returns true if this RecordId falls within the reserved range at the top of the record space.
     */
    bool isReserved() const {
        if (size() != sizeof(uint64_t)) {
            return false;
        }
        return repr() >= kMinReservedRepr && repr() < kMaxRepr;
    }

    // Binary compare data.
    int compare(RecordId other) const {
        // Normal case.
        const int mySize = size();
        const int otherSize = other.size();
        if (mySize == sizeof(uint64_t) && otherSize == sizeof(uint64_t)) {
            return repr() == other.repr() ? 0 : repr() < other.repr() ? -1 : 1;
        }

        // Binary data case.

        const int min = std::min(mySize, otherSize);
        const int cmp = memcmp(_data.c_str(), other.data().c_str(), min);
        if (cmp) {
            return cmp < 0 ? -1 : 1;
        }

        return (mySize == otherSize) ? 0 : mySize < otherSize ? -1 : 1;
    }

    /**
     * Hash value for this RecordId. The hash implementation may be modified, and its behavior
     * may differ across platforms. Hash values should not be persisted.
     */
    struct Hasher {
        size_t operator()(RecordId rid) const {
            size_t hash = 0;
            // TODO consider better hashes
            boost::hash_combine(hash, rid.repr());
            return hash;
        }
    };

    /// members for Sorter
    struct SorterDeserializeSettings {};  // unused
    void serializeForSorter(BufBuilder& buf) const {
        buf.appendNum(static_cast<long long>(repr()));
    }
    static RecordId deserializeForSorter(BufReader& buf, const SorterDeserializeSettings&) {
        return RecordId(buf.read<LittleEndian<int64_t>>());
    }
    int memUsageForSorter() const {
        return sizeof(RecordId);
    }
    RecordId getOwned() const {
        return *this;
    }

private:
    std::string _fromRepr(int64_t repr) {
        return std::string(static_cast<const char*>((void*)&repr), sizeof(int64_t));
    }

    std::string _data;
};

inline bool operator==(RecordId lhs, RecordId rhs) {
    return lhs.compare(rhs) == 0;
}
inline bool operator!=(RecordId lhs, RecordId rhs) {
    return lhs.compare(rhs) != 0;
}
inline bool operator<(RecordId lhs, RecordId rhs) {
    return lhs.compare(rhs) < 0;
}
inline bool operator<=(RecordId lhs, RecordId rhs) {
    return lhs.compare(rhs) <= 0;
}
inline bool operator>(RecordId lhs, RecordId rhs) {
    return lhs.compare(rhs) > 0;
}
inline bool operator>=(RecordId lhs, RecordId rhs) {
    return lhs.compare(rhs) >= 0;
}

inline StringBuilder& operator<<(StringBuilder& stream, const RecordId& id) {
    if (id.size() == sizeof(uint64_t)) {
        return stream << "RecordId(" << id.repr() << ')';
    }
    return stream << "RecordId(" << id.data() << ')';
}

inline std::ostream& operator<<(std::ostream& stream, const RecordId& id) {
    if (id.size() == sizeof(uint64_t)) {
        return stream << "RecordId(" << id.repr() << ')';
    }
    return stream << "RecordId(" << id.data() << ')';
}

inline std::ostream& operator<<(std::ostream& stream, const boost::optional<RecordId>& id) {
    if (id) {
        return stream << *id;
    }
    return stream << "RecordId(0)";
}

inline logger::LogstreamBuilder& operator<<(logger::LogstreamBuilder& stream, const RecordId& id) {
    stream.stream() << id;
    return stream;
}
}  // namespace mongo
