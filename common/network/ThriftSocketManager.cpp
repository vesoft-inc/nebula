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
ThriftSocketManager::getSocket(const HostAddr& host) {
    VLOG(2) << "Getting a socket to "
            << NetworkUtils::intToIPv4(host.first)
            << ":" << host.second;
    auto it = socketMap_->find(host);
    if (it != socketMap_->end()) {
        if (it->second->good()) {
            return it->second;
        }
        LOG(ERROR) << "The existing socket to "
                   << NetworkUtils::intToIPv4(host.first)
                   << ":" << host.second
                   << " is not good, creating a new one";
    }

    // Need to create a new socket
    std::string ipAddr = NetworkUtils::intToIPv4(host.first);
    VLOG(2) << "Trying to create a new socket to "
            << ipAddr << ":" << host.second;
    auto socket = apache::thrift::async::TAsyncSocket::newSocket(
        folly::EventBaseManager::get()->getEventBase(),
        ipAddr,
        host.second,
        FLAGS_conn_timeout_ms);
    for (int i = 0; i < 4; i++) {
        usleep(1000 * FLAGS_conn_timeout_ms / 4);
        if (socket->good()) {
            (*socketMap_)[host] = socket;
            LOG(INFO) << "Succeeded connecting to " << ipAddr
                      << ":" << host.second;
            return socket;
        }
    }

    LOG(ERROR) << "Failed to connect to " << ipAddr << ":" << host.second;
    return nullptr;
}


void ThriftSocketManager::disconnectOnCurrThread() {
    VLOG(1) << "Disconnecting all sockets on the current thread";
    socketMap_->clear();
}

}  // namespace network
}  // namespace vesoft


