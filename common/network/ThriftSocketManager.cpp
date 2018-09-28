/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "network/ThriftSocketManager.h"
#include <folly/io/async/EventBaseManager.h>
#include "network/NetworkUtils.h"

DEFINE_int32(conn_timeout_ms, 1000,
             "Connection timeout in milliseconds");

namespace vesoft {
namespace network {

folly::ThreadLocal<ThriftSocketManager::SocketMap>
    ThriftSocketManager::socketMap_;


std::shared_ptr<apache::thrift::async::TAsyncSocket>
ThriftSocketManager::getSocket(int32_t addr, int32_t port) {
    VLOG(2) << "Getting a socket to " << NetworkUtils::intToIPv4(addr)
            << ":" << port;
    auto it = socketMap_->find(std::make_pair(addr, port));
    if (it != socketMap_->end()) {
        if (it->second->good()) {
            return it->second;
        }
        LOG(ERROR) << "The existing socket to " << NetworkUtils::intToIPv4(addr)
                   << ":" << port << " is no more good, creating a new one";
    }

    // Need to create a new socket
    std::string ipAddr = NetworkUtils::intToIPv4(addr);
    VLOG(2) << "Trying to create a new socket to "
            << ipAddr << ":" << port;
    auto socket = apache::thrift::async::TAsyncSocket::newSocket(
        folly::EventBaseManager::get()->getEventBase(),
        ipAddr,
        port,
        FLAGS_conn_timeout_ms);
    for (int i = 0; i < 4; i++) {
        usleep(1000 * FLAGS_conn_timeout_ms / 4);
        if (socket->good()) {
            (*socketMap_)[std::make_pair(addr, port)] = socket;
            LOG(INFO) << "Succeeded connecting to " << ipAddr << ":" << port;
            return socket;
        }
    }

    LOG(ERROR) << "Failed to connect to " << ipAddr << ":" << port;
    return nullptr;
}


void ThriftSocketManager::disconnectOnCurrThread() {
    VLOG(1) << "Disconnecting all sockets on the current thread";
    socketMap_->clear();
}

}  // namespace network
}  // namespace vesoft


