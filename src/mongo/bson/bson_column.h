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
        static constexpr int valueOffset = 2;   // Type byte and empty field name
        static constexpr int maxValueSize = 8;  // Should be 16 ideally (for Decimal).
        static constexpr int maxElemSize = valueOffset + maxValueSize;
        struct Elem {
            char data[maxElemSize] = {0};
            BSONElement store(BSONElement elem) {
                const char* raw = elem.rawdata();
                data[0] = *raw;
                int valueSize = elem.valuesize();
                invariant(valueSize <= maxValueSize);
                memcpy(data + valueOffset, elem.value(), valueSize);
                return BSONElement(data, 1, valueOffset + valueSize, BSONElement::CachedSizeTag{});
            }
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
            memcpy(elem.data, base.rawdata(), valueOffset);
            memcpy(elem.data + valueOffset, &value, sizeof(value));  // Always copy the entire value

            invariant(deltaIndex <= _store.size());
            if (deltaIndex == _store.size()) {
                _store.push_back(elem);
                BSONElement(_store.back().data, 1, size, BSONElement::CachedSizeTag{});
            }

            invariant(!memcmp(elem.data, _store[deltaIndex].data, sizeof(elem)));  // slow->dassert?
            return BSONElement(
                _store[deltaIndex].data, 1, size + valueOffset, BSONElement::CachedSizeTag{});
        }

        /**
         * Computes a 64-bit delta. Both element values must have the same type and have a value of
         * at most 16 bytes in length. Field names are ignored. A zero return means that either the
         * elements are binary identical, or otherwise invalid for computing a delta.
         */
        static uint64_t calculateDelta(BSONElement base, BSONElement modified) {
            int size = base.valuesize();  // TODO: Fix size > 8 handling
            if (base.type() != modified.type() || size != modified.valuesize() ||
                size > maxValueSize || size == 0)
                return 0;

            // For simplicity, we always use a 64-bit delta.
            uint64_t value = 0;
            uint64_t delta = 0;
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
        /** Parse an Instruction from the stream and return the updated stream pointer. */
        static std::pair<const char*, Instruction> parse(const char* input) {
            Instruction insn;
            while (insn._op = static_cast<uint8_t>(*input++), insn._op >= 128)
                insn._prefix = insn._prefix * 128 + insn._op % 128;  // Accumulate prefix.

            return {input, insn};
        }

        /** Return the smallests possible SetDelta or SetNegDelta instruction. */
        static Instruction makeDelta(uint64_t delta) {
            Instruction pos(Instruction::SetDelta, delta);
            Instruction neg(Instruction::SetNegDelta, -delta);
            // logd("makeDelta: pos = {}, neg = {}", pos, neg);
            return neg.size() < pos.size() ? neg : pos;
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
                    return "{} {:#x} << {}"_format(toString(kind()), _prefix + 1, _op % 16);
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
            out.append(" ]");
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
            return _insn == other._insn && _count == other._count;
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

    std::string toString() const {
        std::string buf;
        for (auto it = begin(); it != end(); ++it)
            buf.append(fmt::format(", {} {}", it.index(), (*it).toString()));
        buf[0] = '{';
        buf.append(" }");
        return buf;
    }

    /**
     * Constructs a BSONColumn, applying delta compression as elements are appended.
     */
    class Builder {
    public:
        ~Builder() {
            done();
        }

        Builder(BufBuilder& baseBuilder, const StringData fieldName)
            : _b(baseBuilder), _offset(baseBuilder.len()) {
            _b.appendNum((char)BinData);
            _b.appendStr(fieldName);
            _b.appendNum((int)0);
            _b.appendNum((char)BinDataType::Column);
            _b.reserveBytes(1);  // Make sure we can add EOO without invalidation.
        }

        /**
         * Append a new element to the column, skipping entries as needed based on the index.
         * The index must be greater than that of the last appended item. Ignores field name.
         */
        void append(int index, BSONElement elem) {
            _maybeUndoDone();
            _emitSkips(index);

            if (!_tryCopy(elem) && !_tryDelta(elem))
                _emitLiteral(elem);

            if (!elem.eoo())
                ++_index;
        }
        /** As above, but append to at the next index without skipping. */
        void append(BSONElement elem) {
            append(_index, elem);
        }

        /** Call to append EOO and update the BinData size. Equivalent to appending EOO. */
        BSONColumn done() {
            if (!_isDone())
                append(BSONElement());
            return BSONColumn(BSONElement(_b.buf() + _offset));
        }

    private:
        void _emitDeferrals() {
            _emitDeferredCopies();
            _emitDeferredDeltas();
        }
        void _emitDeferredCopies() {
            if (_deferrals > 0) {
                Instruction(Instruction::Copy, _deferrals).append(_b);
                _deferrals = 0;
            }
        }
        void _emitDeferredDeltas() {
            if (_deferrals < 0) {
                Instruction(Instruction::Delta, -_deferrals).append(_b);
                _deferrals = 0;
            }
        }

        /** Store a literal BSONElement, resets delta. */
        void _emitLiteral(BSONElement elem) {
            _emitDeferrals();
            int offset = _b.len();
            _b.appendNum((char)elem.type());
            if (elem.type() == BSONType::EOO) {
                _updateBinDataSize();
            } else {
                _b.appendNum((char)0);  // Empty field name
                _b.appendBuf(elem.value(), elem.valuesize());
            }
            int size = _b.len() - offset;
            _last = BSONElement(_b.buf() + offset, 1, size, BSONElement::CachedSizeTag{});
            _delta = 0;
        }

        void _emitSkips(int index) {
            if (index == _index)
                return;

            invariant(index > _index);
            _emitDeferrals();
            Instruction(Instruction::Skip, index - _index).append(_b);
            _index = index;
        }

        bool _isDone() {
            return _last.eoo() && _b.len() < _valueOffset();
        }

        /** Check if Builder::done() was called, and if so, remove EOO and recover state. */
        void _maybeUndoDone() {
            if (!_isDone())
                return;

            logd("undoing done: _last == {}, _b.len() == {}, _offset == {}",
                 _last,
                 _b.len(),
                 _offset);
            _b.setlen(_b.len() - 1);
        }

        int _valueOffset() {
            return _offset + 1 + strlen(_b.buf() + _offset) + sizeof(int) + 1;
        }

        /** Given the last value, tries to add elem using a copy. Returns true iff success. */
        bool _tryCopy(BSONElement elem) {
            if (!elem.binaryEqualValues(_last) || _last.eoo())
                return false;

            _emitDeferredDeltas();

            ++_deferrals;
            return true;
        }

        /** Given the last value, tries to add elem using a delta. Returns true iff success. */
        bool _tryDelta(BSONElement elem) {
            auto delta = DeltaStore::calculateDelta(_last, elem);
            if (!delta)
                return false;

            _emitDeferredCopies();
            // logd("_tryDelta {:x}", delta);

            if (delta == _delta) {
                // Same delta, defer output.
                --_deferrals;
            } else {
                auto instruction = Instruction::makeDelta(delta);
                // Only do the delta if it saves space.
                if (instruction.size() >= elem.size())
                    return false;

                instruction.append(_b);
                _delta = delta;
            }

            _last = _deltaElem.store(elem);
            // logd("delta: _last = {}", _last);
            invariant(_last.rawdata());  // commenting out this useless variant causes a crash...
            return true;
        }

        void _updateBinDataSize() {
            int valueOffset = _valueOffset();
            int size = endian::nativeToLittle(_b.len() - valueOffset);
            memcpy(_b.buf() + valueOffset - 1 - sizeof(int), &size, sizeof(size));
        }

        BufBuilder& _b;               // Buffer to build the column in
        BSONElement _last;            // Last element apended to this column
        uint64_t _delta = 0;          // Current delta
        DeltaStore::Elem _deltaElem;  // Storage for when _last refers to a computed delta element
        int _offset = 0;              // Start of column relative to start of buffer
        int _index = 0;               // Index of next element to add to builder
        int _deferrals = 0;           // Positive indicates number of copies, negative means deltas
    };

private:
    BSONElement _bin;    // Contains the BinData with Column subtype.
    DeltaStore _deltas;  // Owns actual storage for BSON elements stored as deltas.
};                       // namespace mongo
}  // namespace mongo
