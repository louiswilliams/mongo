/*    Copyright 2016 10gen Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#pragma once

#include <vector>

#include "mongo/config.h"
#include "mongo/util/net/abstract_message_port.h"
#include "mongo/util/net/message.h"
#include "mongo/util/net/sock.h"
#include "mongo/transport/shared_memory_stream.h"

namespace mongo {

using transport::SharedMemoryStream;

class MessagingPortSharedMem;

class MessagingPortSharedMem final : public AbstractMessagingPort {
public:
    MessagingPortSharedMem();

    // in some cases the timeout will actually be 2x this value - eg we do a partial send,
    // then the timeout fires, then we try to send again, then the timeout fires again with
    // no data sent, then we detect that the other side is down
    // MessagingPortSharedMem(double so_timeout = 0, logger::LogSeverity logLevel = logger::LogSeverity::Log());

    ~MessagingPortSharedMem() override;

    void setTimeout(Milliseconds millis) override {

    }

    void shutdown() override;

    /* it's assumed if you reuse a message object, that it doesn't cross MessagingPortSharedMem's.
       also, the Message data will go out of scope on the subsequent recv call.
    */
    bool recv(Message& m) override;
    void reply(Message& received, Message& response, int32_t responseToMsgId) override;
    void reply(Message& received, Message& response) override;
    bool call(Message& toSend, Message& response) override;

    void say(Message& toSend, int responseTo = 0) override;
    void say(const Message& toSend) override;

    unsigned remotePort() const override {
        return 0;
    }
    HostAndPort remote() const override {
        return _remoteParsed;
    }
    SockAddr remoteAddr() const override {
        return SockAddr();
    }
    SockAddr localAddr() const override {
        return SockAddr();
    }

    void send(const char* data, int len, const char* context) override;

    void send(const std::vector<std::pair<char*, int>>& data, const char* context) override;

    bool connect(SockAddr& farEnd);

    void setLogLevel(logger::LogSeverity ll) override {
    }

    void clearCounters() override {
    }

    long long getBytesIn() const override {
        return 0;
    }

    long long getBytesOut() const override {
        return 0;
    }

    void setX509SubjectName(const std::string& x509SubjectName) override;

    std::string getX509SubjectName() const override;

    void setConnectionId(const long long connectionId) override;

    long long connectionId() const override;

    void setTag(const AbstractMessagingPort::Tag tag) override;

    AbstractMessagingPort::Tag getTag() const override;

    /**
     * Initiates the TLS/SSL handshake on this MessagingPortSharedMem.
     * When this function returns, further communication on this
     * MessagingPortSharedMem will be encrypted.
     * ssl - Pointer to the global SSLManager.
     * remoteHost - The hostname of the remote server.
     */
    bool secure(SSLManagerInterface* ssl, const std::string& remoteHost) override {
        return false;
    }

    bool isStillConnected() const override {
        return _connected.load();
    }

    uint64_t getSockCreationMicroSec() const override {
        return 0;
    }

private:
    // this is the parsed version of remote
    HostAndPort _remoteParsed;
    std::string _x509SubjectName;
    long long _connectionId;
    AbstractMessagingPort::Tag _tag;
    std::atomic<bool> _connected;
    std::shared_ptr<SharedMemoryStream> _stream;
};

}  // namespace mongo
