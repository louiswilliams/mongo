/**
 *    Copyright (C) 2016 MongoDB Inc.
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

#include <string>

extern "C" {
#include "shmem_stream.h"	
}

#include "mongo/stdx/memory.h"

#pragma once

namespace mongo {
namespace transport {

class SharedMemoryStream  {
public:

	SharedMemoryStream(shmem_stream_t stream) : _stream(stream) {}

	SharedMemoryStream() = default;

	~SharedMemoryStream() {};

	bool connect(std::string name) {
		return (!shmem_stream_connect(const_cast<char*>(name.c_str()), &_stream));
	}

	void receive(char* buffer, int len) {
		shmem_stream_recv(&_stream, buffer, len);
	}

	void send(const char* buffer, int len) {
		shmem_stream_send(&_stream, buffer, len);
	}

	void close() {
		shmem_stream_close(&_stream);
	}

private:
	shmem_stream_t _stream;
};

class SharedMemoryAcceptor {
public:
	SharedMemoryAcceptor(std::string name) : _name(name) {}

	~SharedMemoryAcceptor() {};

	void listen() {
		shmem_stream_listen(const_cast<char*>(_name.c_str()), &_acceptor);
	}

	std::unique_ptr<SharedMemoryStream> accept() {
		shmem_stream_t stream;
		shmem_stream_accept(_acceptor, &stream);
		return stdx::make_unique<SharedMemoryStream>(stream);
	}

	void shutdown() {
		shmem_stream_shutdown(_acceptor);
	}

private:
	shmem_acceptor_t* _acceptor;
	std::string _name;
};

} //transport
} //mongo