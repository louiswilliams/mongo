// record_data.h

/**
 *    Copyright (C) 2014 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include <cstring>

#include "mongo/bson/bsonobjbuilder.h"

namespace mongo {

class UpdateModification {
public:
    class Buffer {
    public:
        explicit Buffer(size_t size) {
            _data = (char*)mongoMalloc(size);
            _size = size;
        }

        // Allocate and copy.
        explicit Buffer(const void* source, size_t size) : Buffer(size) {
            std::memcpy(_data, source, _size);
        }

        Buffer(const Buffer& buf) = delete;
        Buffer(const Buffer&& buf) {
            _data = buf._data;
            _size = buf._size;
        }

        ~Buffer() {
            std::free(_data);
        }

        char* get() {
            return _data;
        }

        size_t size() {
            return _size;
        }

        char* _data;
        size_t _size;
    };

    explicit UpdateModification(Buffer buffer, std::size_t offset, std::size_t replaceSize)
        : _buffer(std::move(buffer)), _offset(offset), _replace(replaceSize){};

    Buffer getOwned() const {
        return std::move(_buffer);
    }

    std::size_t getOffset() const {
        return _offset;
    }
    std::size_t getReplaceSize() const {
        return _replace;
    }

private:
    Buffer _buffer;
    std::size_t _offset;
    std::size_t _replace;
};

}  // namespace mongo
