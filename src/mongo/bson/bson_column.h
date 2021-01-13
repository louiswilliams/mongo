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
 * The BSONColumn is always unowned, but otherwise implements an interface similar to BSONObj.
 */
class BSONColumn {
public:
    BSONColumn() = default;
    BSONColumn(BSONElement bin) : _bin(bin) {
        tassert(0, "invalid BSON type for column", isValid());
    }

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
         * They are in a dereferencable state, and return an EOO BSONElement.
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
            ++_index;

            if (_count > 0)
                --_count;  // Copy
            else if (++_count <= 0)
                _applyDelta();
            else
                do {
                    _nextInsn();    // _count was zero (now one), so process next instruction
                } while (!_count);  // Skip instructions leave count set to zero, so repeat.

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
         * can only be the first element, which is stored in full, or the last (EOO).
         */
        explicit iterator(BSONElement elem) : _cur(elem.rawdata()), _insn(elem.rawdata()) {}
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
                    _count = insn.countArg;
                    break;
                case Instruction::SetNegDelta:
                    insn.deltaArg = -insn.deltaArg;
                    // Fall through.
                case Instruction::SetDelta:
                    _setDelta(insn.deltaArg);
                    _applyDelta();
                    break;
                default:
                    MONGO_UNREACHABLE;
            }
            invariant(_count || insn.kind() == Instruction::Skip);
        }

        void _setDelta(uint64_t delta) {
            _delta = delta;
            if (_cur.rawdata() != _base) {
                int size = _cur.size();
                invariant(static_cast<unsigned>(size) < sizeof(_base));
                memcpy(_base, _cur.rawdata(), size);
                _cur = BSONElement(_base, 1, size, BSONElement::CachedSizeTag{});
            }
        }

        void _applyDelta() {
            // Always apply 64-bit deltas, as we have the full size reserved anyway.
            uint64_t val;
            memcpy(&val, _base + 2, sizeof(val));
            val = endian::littleToNative<uint64_t>(val);
            val += _delta;
            val = endian::nativeToLittle<uint64_t>(val);
            memcpy(_base + 2, &val, sizeof(val));
        }

        BSONElement _cur;     // Defaults to EOO, reference to last full BSONElement or _base
        const char* _insn;    // Pointer to currently executing stream instruction
        int _count = 0;       // number of iterations before advancing to the next instruction
        int _index = 0;       // Position of iterator in the column, including skipped values.
        char _base[18];       // BSON data for elements to apply delta modifications to
        uint64_t _delta = 1;  // Last set delta value to apply to base
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
        return iterator(_bin.eoo() ? _bin : BSONElement(_bin.binData()));
    }

    iterator end() const {
        return iterator(BSONElement(objdata() + objsize() - 1));
    }

private:
    BSONElement _bin;  // Contains the BinData with Column subtype.
};


}  // namespace mongo
