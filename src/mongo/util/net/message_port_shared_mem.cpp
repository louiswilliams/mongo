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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "mongo/platform/basic.h"

#include "mongo/util/net/message_port_shared_mem.h"

#include <fcntl.h>
#include <time.h>

#include "mongo/config.h"
#include "mongo/util/allocator.h"
#include "mongo/util/background.h"
#include "mongo/util/log.h"
#include "mongo/util/net/listen.h"
#include "mongo/util/net/message.h"
#include "mongo/util/net/socket_exception.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/net/ssl_options.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/time_support.h"

#ifndef _WIN32
#ifndef __sun
#include <ifaddrs.h>
#endif
#include <sys/resource.h>
#include <sys/stat.h>
#endif

namespace mongo {

using std::shared_ptr;
using std::string;

/* messagingport -------------------------------------------------------------- */

MessagingPortSharedMem::MessagingPortSharedMem()
    : _stream(std::make_shared<SharedMemoryStream>()) {}

void MessagingPortSharedMem::shutdown() {
    _stream->close();
}

MessagingPortSharedMem::~MessagingPortSharedMem() {
    shutdown();
}

bool MessagingPortSharedMem::recv(Message& message) {

    MSGHEADER::Value header;
    int headerLen = sizeof(MSGHEADER::Value);
    _stream->receive((char*)&header, headerLen);
    int len = header.constView().getMessageLength();

    if (static_cast<size_t>(len) < sizeof(MSGHEADER::Value) ||
        static_cast<size_t>(len) > MaxMessageSizeBytes) {
        LOG(0) << "recv(): message len " << len << " is invalid. "
               << "Min " << sizeof(MSGHEADER::Value) << " Max: " << MaxMessageSizeBytes;
        return false;
    }

    int z = (len + 1023) & 0xfffffc00;
    verify(z >= len);
    auto buf = SharedBuffer::allocate(z);
    MsgData::View md = buf.get();
    memcpy(md.view2ptr(), &header, headerLen);
    int left = len - headerLen;

    _stream->receive(md.data(), left);

    message.setData(std::move(buf));

    return true;
}

void MessagingPortSharedMem::reply(Message& received, Message& response) {
    say(/*received.from, */ response, received.header().getId());
}

void MessagingPortSharedMem::reply(Message& received, Message& response, int32_t responseToMsgId) {
    say(/*received.from, */ response, responseToMsgId);
}

bool MessagingPortSharedMem::call(Message& toSend, Message& response) {
    say(toSend);
    bool success = recv(response);
    if (success) {
        invariant(!response.empty());
        if (response.header().getResponseToMsgId() != toSend.header().getId()) {
            response.reset();
            uasserted(40220, "Response ID did not match the sent message ID.");
        }
    }
    return success;
}

void MessagingPortSharedMem::say(Message& toSend, int responseTo) {
    invariant(!toSend.empty());
    toSend.header().setId(nextMessageId());
    toSend.header().setResponseToMsgId(responseTo);

    return say(const_cast<const Message&>(toSend));
}

void MessagingPortSharedMem::say(const Message& toSend) {
    invariant(!toSend.empty());
    auto buf = toSend.buf();
    if (buf) {
        send(buf, MsgData::ConstView(buf).getLen(), "say");
    }
}

void MessagingPortSharedMem::send(const char* data, int len, const char* context) {
    if (data) {
        _stream->send(data, len);
    }
}

void MessagingPortSharedMem::send(const std::vector<std::pair<char*, int>>& data, const char* context) {
    for (auto it = data.begin(); it < data.end(); it++) {
        send(it->first, it->second, context);
    }
}

bool MessagingPortSharedMem::connect(SockAddr& farEnd) {
    _stream->connect(farEnd.getAddr());
    return true;
}

void MessagingPortSharedMem::setX509SubjectName(const std::string& x509SubjectName) {
    _x509SubjectName = x509SubjectName;
}

std::string MessagingPortSharedMem::getX509SubjectName() const {
    return _x509SubjectName;
}

void MessagingPortSharedMem::setConnectionId(const long long connectionId) {
    _connectionId = connectionId;
}

long long MessagingPortSharedMem::connectionId() const {
    return _connectionId;
}

void MessagingPortSharedMem::setTag(const AbstractMessagingPort::Tag tag) {
    _tag = tag;
}

AbstractMessagingPort::Tag MessagingPortSharedMem::getTag() const {
    return _tag;
}

}  // namespace mongo
