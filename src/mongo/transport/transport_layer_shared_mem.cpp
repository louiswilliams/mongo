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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "transport_layer_shared_mem.h"
#include "mongo/util/log.h"

namespace mongo {
namespace transport {

TicketHolder TransportLayerSharedMem::_ticketHolder(DEFAULT_MAX_CONN);
AtomicInt64 TransportLayerSharedMem::_connectionNumber;

TransportLayerSharedMem::TransportLayerSharedMem(const TransportLayerSharedMem::Options& opts,
                                     std::shared_ptr<ServiceEntryPoint> sep)
    : _sep(sep), _running(false), _options(opts), _acceptor(opts.name) {
    	LOG(0) << "Opening shared memory port on " << opts.name;
    }

TransportLayerSharedMem::ShMemTicket::ShMemTicket(const Session& session, Date_t expiration, WorkHandle work)
    : _sessionId(session.id()), _expiration(expiration), _fill(std::move(work)) {}

Session::Id TransportLayerSharedMem::ShMemTicket::sessionId() const {
    return _sessionId;
}

Date_t TransportLayerSharedMem::ShMemTicket::expiration() const {
    return _expiration;
}


void TransportLayerSharedMem::accepted(std::unique_ptr<SharedMemoryStream> stream) {
    if (!_ticketHolder.tryAcquire()) {
        log() << "connection refused because too many open connections: " << _ticketHolder.used();
        return;
    }

    Session session(HostAndPort(), HostAndPort(), this);

    {
        stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);
        _connections.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(session.id()),
            std::forward_as_tuple(
                std::move(stream), false, session.getTags(), _connectionNumber.addAndFetch(1)));
    }

    invariant(_sep);
    _sep->startSession(std::move(session));
}

void TransportLayerSharedMem::initAndListen() {

    while (!inShutdown() && _running.load()) {

	 	auto stream = _acceptor.accept();

	 	if (stream) {
	 		LOG(1) << "Accepted connection"; 
		
		    std::thread t1(&TransportLayerSharedMem::accepted, this, std::move(stream));
		    t1.detach();
	 	} else {
	 		LOG(1) << "Failed to accept!";
	 	}

    }
}

Status TransportLayerSharedMem::start() {
    if (_running.load()) {
        return {ErrorCodes::InternalError, "TransportLayerSharedMem is already running"};
    }

	if (_options.name.length() == 0) {
		return {ErrorCodes::BadValue, "Must specify name for shared memory file"};
	}
	_acceptor.listen();


    _running.store(true);

    LOG(0) << "Waiting for shared memory connections on " << _options.name;

    _listenerThread = std::thread([this]() { initAndListen(); });

    return Status::OK();
}

Ticket TransportLayerSharedMem::sourceMessage(const Session& session,
                                        Message* message,
                                        Date_t expiration) {
    auto sourceCb = [message](SharedMemoryStream* stream) -> Status {

        MSGHEADER::Value header;
        int headerLen = sizeof(MSGHEADER::Value);
        stream->receive((char*)&header, headerLen);
        int len = header.constView().getMessageLength();

        if (static_cast<size_t>(len) < sizeof(MSGHEADER::Value) ||
            static_cast<size_t>(len) > MaxMessageSizeBytes) {
            LOG(0) << "recv(): message len " << len << " is invalid. "
                   << "Min " << sizeof(MSGHEADER::Value) << " Max: " << MaxMessageSizeBytes;
            return {ErrorCodes::Overflow, "Message len is invalid"};
        }

        int z = (len + 1023) & 0xfffffc00;
        verify(z >= len);
        auto buf = SharedBuffer::allocate(z);
        MsgData::View md = buf.get();
        memcpy(md.view2ptr(), &header, headerLen);
        int left = len - headerLen;

        stream->receive(md.data(), left);

        message->setData(std::move(buf));

        return Status::OK();
    };

    return Ticket(this, stdx::make_unique<ShMemTicket>(session, expiration, std::move(sourceCb)));
}

std::string TransportLayerSharedMem::getX509SubjectName(const Session& session) {
    {
        stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);
        auto conn = _connections.find(session.id());
        if (conn == _connections.end()) {
            // Return empty string if the session is not found
            return "";
        }

        return conn->second.x509SubjectName.value_or("");
    }
}

TransportLayer::Stats TransportLayerSharedMem::sessionStats() {
    Stats stats;
    {
        stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);
        stats.numOpenSessions = _connections.size();
    }

    stats.numAvailableSessions = _ticketHolder.available();
    stats.numCreatedSessions = _connectionNumber.load();

    return stats;
}

Ticket TransportLayerSharedMem::sinkMessage(const Session& session,
                                      const Message& message,
                                      Date_t expiration) {
    auto sinkCb = [&message](SharedMemoryStream* stream) -> Status {
        try {
            invariant(!message.empty());
            auto buf = message.buf();
            if (buf) {
                stream->send(buf, MsgData::ConstView(buf).getLen());
            }
        } catch (const SocketException& e) {
            return {ErrorCodes::HostUnreachable, e.what()};
        }
        return Status::OK();
    };

    return Ticket(this, stdx::make_unique<ShMemTicket>(session, expiration, std::move(sinkCb)));
}

Status TransportLayerSharedMem::wait(Ticket&& ticket) {
    return _runTicket(std::move(ticket));
}

void TransportLayerSharedMem::asyncWait(Ticket&& ticket, TicketCallback callback) {
    // Left unimplemented because there is no reasonable way to offer general async waiting besides
    // offering a background thread that can handle waits for multiple tickets. We may never
    // implement this for the legacy TL.
    MONGO_UNREACHABLE;
}


void TransportLayerSharedMem::end(const Session& session) {
    {
        stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);
        auto conn = _connections.find(session.id());
        if (conn != _connections.end()) {
            _endSession_inlock(conn);
        }
    }
}

void TransportLayerSharedMem::registerTags(const Session& session) {
    {
        stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);
        auto conn = _connections.find(session.id());
        if (conn != _connections.end()) {
            conn->second.tags = session.getTags();
        }
    }
}

void TransportLayerSharedMem::_endSession_inlock(decltype(TransportLayerSharedMem::_connections.begin()) conn) {

    // If the communicator is not there, then it is currently in use.
    if (!(conn->second.stream)) {
        conn->second.ended = true;
    } else {
        _ticketHolder.release();
        _connections.erase(conn);
    }
}

void TransportLayerSharedMem::endAllSessions(Session::TagMask tags) {
    log() << "transport layer, ending all sessions";
    {
        stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);
        auto&& conn = _connections.begin();
        while (conn != _connections.end()) {
            // If we erase this connection below, we invalidate our iterator, use a placeholder.
            auto placeholder = conn;
            placeholder++;

            if (conn->second.tags & tags) {
                log() << "Skip closing connection for connection # " << conn->second.connectionId;
            } else {
                _endSession_inlock(conn);
            }

            conn = placeholder;
        }
    }
}

void TransportLayerSharedMem::shutdown() {
    
    if (_running.load()) {
    	_running.store(false);
	    // stop the listener??

	    _acceptor.shutdown();

	    _listenerThread.join();
	    endAllSessions();
	}
}

Status TransportLayerSharedMem::_runTicket(Ticket ticket) {
    if (!_running.load()) {
        return {ErrorCodes::ShutdownInProgress, "TransportLayer in shutdown"};
    }

    if (ticket.expiration() < Date_t::now()) {
        return {ErrorCodes::ExceededTimeLimit, "Ticket has expired"};
    }

    // TODO not sure if bringing out connection like this will be okay
    // fix thi sto stdmove

    std::unique_ptr<SharedMemoryStream> stream;

    {
        stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);

        auto conn = _connections.find(ticket.sessionId());
        if (conn == _connections.end()) {
            return {ErrorCodes::TransportSessionNotFound, "No such session in TransportLayer"};
        }

        stream = std::move(conn->second.stream);
    }

    auto shMemTicket = dynamic_cast<ShMemTicket*>(getTicketImpl(ticket));
    auto res = shMemTicket->_fill(stream.get());

    {
        stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);

        auto conn = _connections.find(ticket.sessionId());
        invariant(conn != _connections.end());

        conn->second.stream = std::move(stream);
        if (conn->second.ended) {
            _endSession_inlock(conn);
        }
    }

    return res;
}

} //transport
} //mongo
