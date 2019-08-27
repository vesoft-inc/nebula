/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp2/async/ReconnectingRequestChannel.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <folly/system/ThreadName.h>
#include "network/NetworkUtils.h"

DECLARE_int32(conn_timeout_ms);

namespace nebula {
namespace thrift {

template<class ClientType>
std::shared_ptr<ClientType> ThriftClientManager<ClientType>::client(
        const HostAddr& host, folly::EventBase* evb, bool compatibility, uint32_t timeout) {
    VLOG(2) << "Getting a client to "
            << network::NetworkUtils::intToIPv4(host.first)
            << ":" << host.second;

    if (evb == nullptr) {
        evb = folly::EventBaseManager::get()->getEventBase();
    }

    auto it = clientMap_->find(std::make_pair(host, evb));
    if (it != clientMap_->end()) {
        return it->second;
    }

    // Need to create a new client
    auto ipAddr = network::NetworkUtils::intToIPv4(host.first);
    auto port = host.second;
    VLOG(2) << "There is no existing client to "
            << ipAddr << ":" << port
            << ", trying to create one";
    auto channel = apache::thrift::ReconnectingRequestChannel::newChannel(
        *evb, [compatibility, ipAddr, port, timeout] (folly::EventBase& eb) mutable {
            static thread_local int connectionCount = 0;
            VLOG(2) << "Connecting to " << ipAddr << ":" << port
                    << " for " << ++connectionCount << " times";
            std::shared_ptr<apache::thrift::async::TAsyncSocket> socket;
            eb.runImmediatelyOrRunInEventBaseThreadAndWait(
                [&socket, &eb, ipAddr, port]() {
                    socket = apache::thrift::async::TAsyncSocket::newSocket(
                        &eb, ipAddr, port, FLAGS_conn_timeout_ms);
                });
            auto headerClientChannel =  apache::thrift::HeaderClientChannel::newChannel(socket);
            if (timeout > 0) {
                headerClientChannel->setTimeout(timeout);
            }
            if (compatibility) {
                headerClientChannel->setProtocolId(apache::thrift::protocol::T_BINARY_PROTOCOL);
                headerClientChannel->setClientType(THRIFT_UNFRAMED_DEPRECATED);
            }
            return headerClientChannel;
        });
    std::shared_ptr<ClientType> client(new ClientType(std::move(channel)), [evb](auto* p) {
        evb->runImmediatelyOrRunInEventBaseThreadAndWait([p] {
            delete p;
        });
    });
    clientMap_->emplace(std::make_pair(host, evb), client);
    return client;
}

}  // namespace thrift
}  // namespace nebula


