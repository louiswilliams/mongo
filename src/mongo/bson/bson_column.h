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

#pragma once

#include <deque>
#include <memory>

#include <fmt/format.h>

#include "mongo/base/data_type.h"
#include "mongo/base/string_data.h"
#include "mongo/base/string_data_comparator_interface.h"
#include "mongo/bson/bson_comparator_interface_base.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/oid.h"
#include "mongo/bson/timestamp.h"
#include "mongo/bson/util/builder.h"
#include "mongo/logv2/log_debug.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/platform/endian.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/bufreader.h"
#include "mongo/util/shared_buffer.h"
#include "mongo/util/string_map.h"

namespace mongo {

/**
 * The BSONColumn class represents a reference to a BSONElement of BinDataType 7, which can
 * efficiently store any BSONArray and also allows for missing values. At a high level, three
 * optimizations are applied:
 *   - implied field names: do not store decimal keys representing index keys.
 *   - variable sized scalars: avoid storing unset bytes in fixed size BSON types
 *   - delta compression: store difference between subsequent scalars of the same type
 *   - run length encoding: repeated identical deltas or elements use constant size storage
 * Delta values and repetition counts are variable sized, so the savings compound.
 *
 * The BSONColumn will not take ownership of the BinData element, but otherwise implements
 * an interface similar to BSONObj. Because iterators over the BSONColumn need to rematerialize
 * deltas, they use additional storage owned by the BSONColumn for this. As all iterators will
 * create new delta's in the same order, they share a single DeltaStore, with a worst-case memory
 * usage bounded to a total size  on the order of that of the size of the expanded BSONColumn.
 */
class BSONColumn {
public:
    BSONColumn() = default;
    BSONColumn(BSONElement bin) : _bin(bin) {
        tassert(0, "invalid BSON type for column", isValid());
    }

    /**
     * Class to manage application of deltas between small BSONElements in the context of a
     * BSONColumn. Owns all storage referred to by returned BSONElement values. Repeated
     * delta applications for the same DeltaStore iterator must yield the same result.
     */
    class DeltaStore {
    public:
        static constexpr int valueOffset = 2;   // Type byte and field name
        static constexpr int maxValueSize = 8;  // Should be 16 ideally (for Decimal).
        static constexpr int maxElemSize = valueOffset + maxValueSize;
        struct Elem {
            char elem[maxElemSize] = {0};
        };
        using iterator = std::deque<Elem>::iterator;

        BSONElement applyDelta(unsigned deltaIndex, BSONElement base, uint64_t delta) {
            int size = base.valuesize();  // TODO: Fix size > 8 handling.
            invariant(static_cast<size_t>(size) <= maxValueSize);

            // For simplicity, always apply the delta using the maximum 64-bit size
            uint64_t value;
            memcpy(&value, base.value(), size);
            value = endian::littleToNative(value);
            value += delta;
            value = endian::nativeToLittle(value);

            // Copy the empty field name and type byte from the base, followed by the new value.
            Elem elem;
            memcpy(elem.elem, base.rawdata(), valueOffset);
            memcpy(elem.elem + valueOffset, &value, sizeof(value));  // Always copy the entire value

            invariant(deltaIndex <= _store.size());
            if (deltaIndex == _store.size()) {
                _store.push_back(elem);
                BSONElement(_store.back().elem, 1, size, BSONElement::CachedSizeTag{});
            }

            invariant(!memcmp(elem.elem, _store[deltaIndex].elem, sizeof(elem)));  // slow->dassert?
            return BSONElement(
                _store[deltaIndex].elem, 1, size + valueOffset, BSONElement::CachedSizeTag{});
        }

        /**
         * Computes a 64-bit delta. Both element values must have the same type and have a value of
         * at most 16 bytes in length. Field names are ignored. A zero return means that either the
         * elements are binary identical, or otherwise invalid for computing a delta.
         */

        static uint64_t calculateDelta(BSONElement base, BSONElement modified) {
            int size = base.valuesize();  // TODO: Fix size > 8 handling
            if (base.type() != modified.type() || size != modified.valuesize() ||
                size > maxValueSize)
                return 0;

            // For simplicity, we always use a 64-bit delta.
            uint64_t value;
            uint64_t delta;
            memcpy(&value, base.value(), size);
            value = endian::littleToNative(value);
            memcpy(&delta, modified.value(), size);
            delta = endian::littleToNative(delta);
            delta -= value;

            return delta;
        }

        iterator begin() {
            return _store.begin();
        }

    private:
        std::deque<Elem> _store;
    };

    /** represents a parsed stream instruction for a BSON column */
    class Instruction {
    public:
        enum Kind { Literal0, Literal1, Skip, Delta, Copy, SetNegDelta, SetDelta };

        static StringData toString(Kind k) {
            StringData strings[] = {"Literal0"_sd,
                                    "Literal1"_sd,
                                    "Skip"_sd,
                                    "Delta"_sd,
                                    "Copy"_sd,
                                    "SetNegDelta"_sd,
                                    "SetDelta"_sd};
            return strings[k];
        }
        Instruction() = default;
        Instruction(Kind kind, uint64_t arg) {
            // logd("new insn:  kind = {}, arg = {:x}", kind, arg);
            switch (kind) {
                case Skip:
                case Delta:
                case Copy:
                    _prefix = arg / 16;
                    _op = kind * 16 + arg % 16;
                    break;
                case SetNegDelta:
                case SetDelta:
                    invariant(arg);
                    _op = kind * 16;
                    while (arg % 16 == 0 && _op % 16 < 15) {
                        ++_op;
                        arg /= 16;
                    }
                    invariant(arg);
                    _prefix = arg - 1;
                    break;
                default:
                    MONGO_UNREACHABLE;
            }
        }
        static std::pair<const char*, Instruction> parse(const char* input) {
            Instruction insn;
            while (insn._op = static_cast<uint8_t>(*input++), insn._op >= 128)
                insn._prefix = insn._prefix * 128 + insn._op % 128;  // Accumulate prefix.

            return {input, insn};
        }

        std::string toString() const {
            using namespace fmt::literals;
            switch (kind()) {
                case Literal0:
                case Literal1:
                    return "Literal {}"_format(typeName(static_cast<BSONType>(op())));
                case Skip:
                case Delta:
                case Copy:
                    return "{} {}"_format(toString(kind()), countArg());
                case SetNegDelta:
                case SetDelta:
                    return "{} {:#x}"_format(toString(kind()), endian::nativeToLittle(deltaArg()));
            }
        }

        /** Append a non-literal instruction to the builder */
        void append(BufBuilder& builder) {
            // logd("append: prefix = {:x}, op = {:x}", _prefix, _op);
            char buf[11];  // At most 10 prefix bytes and an op byte
            char* end = buf + sizeof(buf);
            char* begin = end;

            *--begin = _op;
            while (_prefix) {
                *--begin = _prefix % 128 + 128;
                _prefix /= 128;
            }
            builder.appendBuf(begin, end - begin);
        }

        static std::string disassemble(const char* data, int len) {
            const char* end = data + len;
            std::string out = "[ ";
            while (data != end) {
                if (!*data) {
                    out.append("EOO");
                    break;
                }

                auto [next, insn] = Instruction::parse(data);
                if (insn.kind() <= Instruction::Kind::Literal1)
                    next += BSONElement(next - 1).size() - 1;
                data = next;

                out.append(insn.toString());
                out.append(", ");
            }
            out.append("]");
            return out;
        }

        Kind kind() const {
            return static_cast<Kind>(_op / 16);
        }

        int size() const {
            int size = 1;
            uint8_t op = _op;
            uint64_t prefix = _prefix;
            while (op && prefix) {
                ++size;
                prefix /= 128;
            }
            return size;
        }

        uint8_t op() const {
            return _op;
        }

        uint64_t countArg() const {
            return _prefix * 16 + _op % 16;
        }

        uint64_t deltaArg() const {
            return (_prefix + 1) << (_op % 16 * 4);
        }

    private:
        uint8_t _op = 0;  // Parsed operation. For literal BSON elements, this is the BSON type.
        uint64_t _prefix = 0;
    };

    /** Like BSONObjStlIterator */
    class iterator {
    public:
        using iterator_category = std::forward_iterator_tag;
        using difference_type = ptrdiff_t;
        using value_type = BSONElement;
        using pointer = const BSONElement*;
        using reference = const BSONElement&;

        /**
         * All default constructed iterators are equal to each other.
         * They are in a dereferencable state, and return the EOO BSONElement.
         * They must not be incremented.
         */
        iterator() = default;

        const BSONElement& operator*() const {
            return _cur;
        }

        const BSONElement* operator->() const {
            return &operator*();
        }

        bool operator==(const iterator& other) {
            return operator*().rawdata() == other.operator*().rawdata();
        }

        bool operator!=(const iterator& other) {
            return !(*this == other);
        }

        int index() {
            return _index;
        }

        /** pre-increment */
        iterator& operator++() {
            while (!_count)
                _nextInsn();
            ++_index;

            if (_count > 0)
                --_count;  // Copy
            else {
                ++_count;
                _cur = _store->applyDelta(_deltaIndex++, _cur, _delta);  // Delta
            }
            // logd("advance: {}", *this);
            return *this;
        }

        /** post-increment */
        iterator operator++(int) {
            iterator oldPos = *this;
            ++*this;
            return oldPos;
        }

        iterator& nextDifferent() {
            if (_count > 0) {
                _index += _count;
                _count = 0;
            }
            return operator++();
        }

        std::string toString() const {
            return fmt::format(
                "iterator: _cur = {}, _count = {}, _index = {}", _cur.toString(), _count, _index);
        }

    private:
        friend class BSONColumn;
        /**
         * Create an iterator pointing at the given element within the BSONColumn. Note that this
         * can only be the first element, which is stored in full, or the last (EOO). The deltaOut
         * iterator is needed to ensure that storage of materialized deltas lives for the life-time
         * of the BSONColumn.
         */
        explicit iterator(BSONElement elem, DeltaStore* store)
            : _cur(elem.rawdata()), _insn(elem.rawdata() + elem.size()), _store(store) {}

        void _nextInsn() {
            Instruction insn;
            std::tie(_insn, insn) = Instruction::parse(_insn);

            switch (insn.kind()) {
                case Instruction::Literal0:
                case Instruction::Literal1:
                    invariant(!insn.op() || !*_insn, "expected BSONElement with empty name");
                    _cur = BSONElement(--_insn, 1, -1, BSONElement::CachedSizeTag{});
                    _count = 1;
                    _insn += _cur.size();
                    break;
                case Instruction::Skip:
                    _index += insn.countArg();
                    break;
                case Instruction::Delta:
                    _count = -insn.countArg();
                    break;
                case Instruction::Copy:
                    _count = insn.countArg();
                    break;
                case Instruction::SetNegDelta:
                    _delta = -insn.deltaArg();
                    _cur = _store->applyDelta(_deltaIndex++, _cur, _delta);
                    _count = 1;
                    break;
                case Instruction::SetDelta:
                    _delta = insn.deltaArg();
                    _cur = _store->applyDelta(_deltaIndex++, _cur, _delta);
                    _count = 1;
                    break;
            }
            // logd("_nextInsn: insn = {}, this = {}", insn, *this);
            invariant(_count || insn.kind() == Instruction::Skip);
        }

        BSONElement _cur;          // Defaults to EOO, reference to last full BSONElement or _base
        const char* _insn;         // Pointer to currently executing stream instruction
        int _count = 0;            // Number of iterations before advancing to the next instruction
        int _index = 0;            // Position of iterator in the column, including skipped values
        DeltaStore* _store;        // Manages storage for applied deltas
        unsigned _deltaIndex = 0;  // Index into _store for next delta application
        uint64_t _delta = 1;       // Last set delta value to apply to base
    };

    using const_iterator = iterator;

    /**
     * Basic sanity check that the BSONColumn is well-formed and safe for iteration.
     */
    bool isValid() const {
        return _bin.eoo() ||
            (_bin.type() == BinData && _bin.binDataType() == Column && objsize() >= 0 &&
             objsize() <= BSONObjMaxUserSize &&
             static_cast<BSONType>(objdata()[objsize() - 1]) == EOO);
    }

    int nFields() const {
        auto it = begin();
        auto endIt = end();
        int count = 0;
        while (it != endIt) {
            ++count;
            ++it;  // TODO: Should skip ahead for copied fields.
        }
        return count;
    }

    BSONElement operator[](int field) const {
        auto it = begin();
        auto endIt = end();
        while (it != endIt && it.index() != field)
            ++it;
        return *it;
    }

    const char* objdata() const {
        return _bin.rawdata();
    }

    int objsize() const {
        return _bin.size();
    }

    bool isEmpty() const {
        return objsize() <= 5;  // Empty array is 4 bytes for length + EOO.
    }

    iterator begin() const {
        auto elem = BSONElement(_bin.eoo() ? _bin : BSONElement(_bin.binData()));
        return iterator(elem, const_cast<DeltaStore*>(&_deltas));
    }

    iterator end() const {
        return iterator(BSONElement(objdata() + objsize() - 1), const_cast<DeltaStore*>(&_deltas));
    }

    /**
     * Constructs a BSONColumn, applying delta compression as elements are appended.
     */
    class Builder {
    public:
        ~Builder() {
            _doDeferrals();
            _b.appendNum((char)EOO);
            _updateBinDataSize();
        }

        Builder(BufBuilder& baseBuilder, const StringData fieldName)
            : _b(baseBuilder), _offset(baseBuilder.len()) {
            _b.appendNum((char)BinData);
            _b.appendStr(fieldName);
            _sizeOffset = _b.len();
            _b.appendNum((int)0);
            _b.appendNum((char)BinDataType::Column);
        }

        /**
         * Append a new element to the column, skipping entries as needed based on the index.
         * The index must be greater than that of the last appended item. Ignores field name.
         */
        void append(int index, BSONElement elem) {
            invariant(index >= _index);
            if (index > _index) {
                _doDeferrals();
                Instruction(Instruction::Skip, index - _index).append(_b);
                _index = index;
            }
            BSONElement last = _lastLiteral
                ? BSONElement(_b.buf() + _lastLiteral, 1, -1, BSONElement::CachedSizeTag())
                : BSONElement();

            // Try Copy
            if (elem.binaryEqualValues(last)) {
                _doDeltas();
                ++_deferrals;
                ++_index;
                return;
            }
            // Try Delta
            if (auto delta = DeltaStore::calculateDelta(last, elem)) {
                _doCopies();
                // Still need to handle the case of consecutive deltas.
                // logd("calculated a delta of {:x}", delta);
                Instruction pos(Instruction::SetDelta, delta);
                Instruction neg(Instruction::SetNegDelta, -delta);
                Instruction min = neg.size() < pos.size() ? neg : pos;
                // Only do the delta if it saves space.
                if (min.size() < last.valuesize()) {
                    min.append(_b);
                    ++_index;
                    _delta = delta;
                    return;
                }
            }
            // Store literal BSONElement, resets delta.
            _doDeferrals();
            _lastLiteral = _b.len();
            _b.appendNum((char)elem.type());
            _b.appendNum((char)0);
            _b.appendBuf(elem.value(), elem.valuesize());

            // logd("set _lastLiteral to {}", BSONElement(_b.buf() + _lastLiteral));
            ++_index;
            _delta = 1;
        }


    private:
        void _doDeferrals() {
            _doCopies();
            _doDeltas();
        }
        void _doCopies() {
            if (_deferrals <= 0)
                return;
            Instruction(Instruction::Copy, static_cast<uint64_t>(_deferrals)).append(_b);
            _deferrals = 0;
        }
        void _doDeltas() {
            if (_deferrals >= 0)
                return;
            Instruction(Instruction::Skip, static_cast<uint64_t>(-_deferrals)).append(_b);
            _deferrals = 0;
        }
        void _updateBinDataSize() {
            int32_t size = endian::nativeToLittle(_b.len());
            memcpy(_b.buf() + _sizeOffset, &size, sizeof(size));
        }

        BufBuilder& _b;        // Buffer to build the column in
        int _offset = 0;       // Start of column relative to start of buffer
        int _index = 0;        // Index of next element to add to builder
        int _deferrals = 0;    // Positive indicates number of copies, negative means deltas
        int _sizeOffset = 0;   // Offset to location for storing final BinData size
        int _lastLiteral = 0;  // Last literal element appended to this column
        uint64_t _delta = 0;   // Current delta
    };

private:
    BSONElement _bin;    // Contains the BinData with Column subtype.
    DeltaStore _deltas;  // Owns actual storage for BSON elements stored as deltas.
};                       // namespace mongo
}  // namespace mongo
