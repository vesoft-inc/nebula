/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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
        const HostAddr& host, folly::EventBase* evb) {
    VLOG(2) << "Getting a client to " << host;

    if (evb == nullptr) {
        // NOTE: This is an event base for the current thread
        evb = folly::EventBaseManager::get()->getEventBase();
    }

    // Get the thread-local client map for the current thread.
    auto *clients = clientMap_.get();

    auto it = clients->find(std::make_pair(host, evb));
    if (it != clients->end()) {
        return it->second;
    }

    // Need to create a new client
    VLOG(2) << "There is no existing client to " << host
            << ", trying to create one";

    using apache::thrift::HeaderClientChannel;
    using apache::thrift::ReconnectingRequestChannel;
    using apache::thrift::async::TAsyncSocket;

    // To make sure the channel would be destroyed in its belonging event base thread,
    // we must provide a customized deleter.
    auto deleter = [evb] (auto *channel) {
        auto cb = [=] () {
            channel->destroy();
        };
        if (evb->isInEventBaseThread()) {
            cb();
        } else if (evb->isRunning()) {
            evb->runInEventBaseThread(cb);
        }
    };

    auto creator = [host, deleter] (auto &eb) mutable {
        static thread_local int connectionCount = 0;
        VLOG(2) << "Connecting to " << host
                << " for " << ++connectionCount << " times";

        // The TAsyncSocket must be created from inside the event base thread
        std::shared_ptr<TAsyncSocket> socket;
        eb.runImmediatelyOrRunInEventBaseThreadAndWait([&socket, &eb, host] () {
            auto ip = network::NetworkUtils::intToIPv4(host.first);
            auto port = host.second;
            socket = TAsyncSocket::newSocket(&eb, ip, port, FLAGS_conn_timeout_ms);
        });

        auto *channel = new HeaderClientChannel(std::move(socket));
        return std::shared_ptr<HeaderClientChannel>(channel, deleter);
    };

    auto channel = ReconnectingRequestChannel::newChannel(*evb, creator);
    auto client = std::make_shared<ClientType>(std::move(channel));
    clients->emplace(std::make_pair(host, evb), client);
    return client;
}


template <class ClientType>
void ThriftClientManager<ClientType>::destroyAll() {
    VLOG(2) << "Destroying all clients.";
    for (auto &clients : clientMap_.accessAllThreads()) {
        clients.clear();
    }
    VLOG(2) << "All clients are destroyed.";
}

}  // namespace thrift
}  // namespace nebula


