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

#include <unordered_map>

#include "shared_memory_stream.h"

#include "mongo/platform/basic.h"
#include "mongo/db/service_context.h"
#include "mongo/db/stats/counters.h"
#include "mongo/util/net/message.h"
#include "mongo/transport/session.h"
#include "mongo/transport/service_entry_point.h"
#include "mongo/transport/ticket_impl.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/concurrency/ticketholder.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/net/listen.h"
#include "mongo/util/net/socket_exception.h"


#include "mongo/stdx/memory.h"
#include "mongo/stdx/thread.h"


namespace mongo {
namespace transport {

class TransportLayerSharedMem : public TransportLayer {
public:
    
    struct Options {
        std::string name;  // named segment to listen on

        Options() : name("") {}
    };

    TransportLayerSharedMem(const TransportLayerSharedMem::Options& opts,
                            std::shared_ptr<ServiceEntryPoint> sep);

    ~TransportLayerSharedMem() {
        shutdown();
    }

     Ticket sourceMessage(const Session& session,
                                 Message* message,
                                 Date_t expiration = Ticket::kNoExpirationDate) override;

     Ticket sinkMessage(const Session& session,
                               const Message& message,
                               Date_t expiration = Ticket::kNoExpirationDate) override;

     Status wait(Ticket&& ticket) override;

    using TicketCallback = stdx::function<void(Status)>;

     void asyncWait(Ticket&& ticket, TicketCallback callback) override;

     void registerTags(const Session& session) override;

     std::string getX509SubjectName(const Session& session) override;

     Stats sessionStats() override;

     void end(const Session& session) override;

     void endAllSessions(Session::TagMask tags = Session::kEmptyTagMask) override;

     Status start() override;

     void shutdown() override;
private:

    void initAndListen();

    void accepted(std::unique_ptr<SharedMemoryStream> stream);

    Status _runTicket(Ticket ticket);

    using WorkHandle = stdx::function<Status(SharedMemoryStream*)>;

    /**
     * A TicketImpl implementation for this TransportLayer. WorkHandle is a callable that
     * can be invoked to fill this ticket.
     */
    class ShMemTicket : public TicketImpl {
        MONGO_DISALLOW_COPYING(ShMemTicket);

    public:
        ShMemTicket(const Session& session, Date_t expiration, WorkHandle work);

        SessionId sessionId() const override;
        Date_t expiration() const override;

        SessionId _sessionId;
        Date_t _expiration;

        WorkHandle _fill;
    };

    /**
     * Connection object, to associate Session ids with AbstractMessagingPorts.
     */
    struct Connection {
        Connection(std::unique_ptr<SharedMemoryStream> stream,
                   bool ended,
                   Session::TagMask tags,
                   long connectionId)
            : stream(std::move(stream)),
              connectionId(connectionId),
              inUse(false),
              ended(false),
              tags(tags) {}

        std::unique_ptr<SharedMemoryStream> stream;
        const long long connectionId;
        boost::optional<std::string> x509SubjectName;
        bool inUse;
        bool ended;
        Session::TagMask tags;
    };

    std::shared_ptr<ServiceEntryPoint> _sep;

    stdx::thread _listenerThread;

    stdx::mutex _connectionsMutex;
    std::unordered_map<Session::Id, Connection> _connections;

    void _endSession_inlock(decltype(_connections.begin()) conn);

    AtomicWord<bool> _running;
    static AtomicInt64 _connectionNumber;
    static TicketHolder _ticketHolder;

    Options _options;
    SharedMemoryAcceptor _acceptor;
};

} //transport
} //mongo
