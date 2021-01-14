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

#include "mongo/base/data_type.h"
#include "mongo/base/string_data.h"
#include "mongo/base/string_data_comparator_interface.h"
#include "mongo/bson/bson_comparator_interface_base.h"
#include "mongo/bson/bsonelement.h"
#include "mongo/bson/bsontypes.h"
#include "mongo/bson/oid.h"
#include "mongo/bson/timestamp.h"
#include "mongo/bson/util/builder.h"
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
 * create new delta's in the same order, they share a single DeltaWriter, with a worst-case memory
 * usage bounded a total size  on the order of that of the size of the expanded BSONColumn.
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
        static constexpr int maxElemSize = 18;
        static constexpr int valueOffset = 2;  // Type byte and field name
        struct Elem {
            char elem[maxElemSize] = {0};
        };
        using iterator = std::deque<Elem>::iterator;

        BSONElement applyDelta(unsigned deltaIndex, BSONElement base, uint64_t delta) {
            int size = base.valuesize();
            invariant(static_cast<size_t>(size) <= maxElemSize);

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

        // Both element values must be at most 16 bytes in size
        uint64_t calculateDelta(BSONElement base, BSONElement modified);

        iterator begin() {
            return _store.begin();
        }

    private:
        std::deque<Elem> _store;
    };

    /**
     * Like BSONObjStlIterator, but returns EOO elements for skipped values.
     */
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

        /** represents a parsed stream instruction for a BSON column */
        struct Instruction {
            static std::pair<const char*, Instruction> parse(const char* input) {
                uint8_t op;
                uint64_t prefix = 0;
                while (op = static_cast<uint8_t>(*input++), op >= 128)
                    prefix = prefix * 128 + op % 128;  // Accumulate prefix.

                return {input, {op, prefix * 16 + op % 16, (prefix + 1) << (op % 16 * 4)}};
            }

            enum Kind { Literal0, Literal1, Skip, Delta, Copy, SetNegDelta, SetDelta };
            Kind kind() {
                return static_cast<Kind>(op / 16);
            }

            uint8_t op = 0;  // Parsed operation. For literal BSON elements, this is the BSON type.
            uint64_t countArg;  // Which of countArg and deltaArg is applicable depends on the op.
            uint64_t deltaArg;
        };

    private:
        friend class BSONColumn;
        /**
         * Create an iterator pointing at the given element within the BSONColumn. Note that this
         * can only be the first element, which is stored in full, or the last (EOO). The deltaOut
         * iterator is needed to ensure that storage of materialized deltas lives for the life-time
         * of the BSONColumn.
         */
        explicit iterator(BSONElement elem, DeltaStore* store)
            : _cur(elem.rawdata()), _insn(elem.rawdata()), _store(store) {}
        void _nextInsn() {
            Instruction insn;
            std::tie(_insn, insn) = Instruction::parse(_insn);

            switch (insn.kind()) {
                case Instruction::Literal0:
                case Instruction::Literal1:
                    invariant(!insn.op || !*_insn, "expected BSONElement with empty name");
                    _cur = BSONElement(--_insn, 1, -1, BSONElement::CachedSizeTag{});
                    _count = 1;
                    _insn += _cur.size();
                    break;
                case Instruction::Skip:
                    _index += insn.countArg;
                    insn.countArg = 0;
                    // Fall through.
                case Instruction::Delta:
                    insn.countArg = -insn.countArg;
                    // Fall through.
                case Instruction::Copy:
                    invariant(insn.countArg || insn.kind() == Instruction::Skip);
                    _count = insn.countArg - 1;
                    break;
                case Instruction::SetNegDelta:
                    insn.deltaArg = -insn.deltaArg;
                    // Fall through.
                case Instruction::SetDelta:
                    _delta = insn.deltaArg;
                    _cur = _store->applyDelta(_deltaIndex++, _cur, _delta);
                    _count = 1;
                    break;
                default:
                    MONGO_UNREACHABLE;
            }
            // invariant(_count || insn.kind() == Instruction::Skip);
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
        while (it != endIt)
            ++count, ++it;  // TODO: Should skip ahead for copied fields.
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

private:
    BSONElement _bin;    // Contains the BinData with Column subtype.
    DeltaStore _deltas;  // Owns actual storage for BSON elements stored as deltas.
};


}  // namespace mongo
