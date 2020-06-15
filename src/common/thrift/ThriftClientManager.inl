/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <thrift/lib/cpp2/async/ReconnectingRequestChannel.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>
#include <thrift/lib/cpp/async/TAsyncSocket.h>
#include <folly/system/ThreadName.h>
#include "common/network/NetworkUtils.h"

DECLARE_int32(conn_timeout_ms);

namespace nebula {
namespace thrift {

template<class ClientType>
std::shared_ptr<ClientType> ThriftClientManager<ClientType>::client(
        const HostAddr& host, folly::EventBase* evb, bool compatibility, uint32_t timeout) {
    VLOG(2) << "Getting a client to " << host;

    if (evb == nullptr) {
        evb = folly::EventBaseManager::get()->getEventBase();
    }

    auto it = clientMap_->find(std::make_pair(host, evb));
    if (it != clientMap_->end()) {
        return it->second;
    }

    // Need to create a new client
    VLOG(2) << "There is no existing client to " << host << ", trying to create one";
    auto channel = apache::thrift::ReconnectingRequestChannel::newChannel(
        *evb, [compatibility, host = host, timeout] (folly::EventBase& eb) mutable {
            static thread_local int connectionCount = 0;

            /*
             * TODO(liuyu): folly said 'resolve' may take second to finish
             *              if this really happen, we will add a cache here.
             * */
            if (!folly::IPAddress::validate(host.host)) {
                try {
                    folly::SocketAddress socketAddr(host.host, host.port, true);
                    std::ostringstream oss;
                    oss << "resolve " << host << " as ";
                    host.host = socketAddr.getAddressStr();
                    oss << host;
                    LOG(INFO) << oss.str();
                } catch(const std::exception& e) {
                    LOG(ERROR) << e.what();
                }
            }

            VLOG(2) << "Connecting to " << host << " for " << ++connectionCount << " times";
            std::shared_ptr<apache::thrift::async::TAsyncSocket> socket;
            eb.runImmediatelyOrRunInEventBaseThreadAndWait(
                [&socket, &eb, host]() {
                    socket = apache::thrift::async::TAsyncSocket::newSocket(
                        &eb, host.host, host.port, FLAGS_conn_timeout_ms);
                });
            auto headerClientChannel = apache::thrift::HeaderClientChannel::newChannel(socket);
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
