/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_THRIFT_THRIFTCLIENTMANAGER_INL_H
#define COMMON_THRIFT_THRIFTCLIENTMANAGER_INL_H

#include <folly/io/async/AsyncSSLSocket.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/system/ThreadName.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>

#include "common/base/Base.h"
#include "common/network/NetworkUtils.h"
#include "common/ssl/SSLConfig.h"

DECLARE_int32(conn_timeout_ms);

namespace nebula {
namespace thrift {
template <class ClientType>
std::shared_ptr<ClientType> ThriftClientManager<ClientType>::client(const HostAddr& host,
                                                                    folly::EventBase* evb,
                                                                    bool compatibility,
                                                                    uint32_t timeout) {
  if (evb == nullptr) {
    evb = folly::EventBaseManager::get()->getEventBase();
  }
  // Get client from client manager if it is ok.
  auto it = clientMap_->find(std::make_pair(host, evb));
  if (it != clientMap_->end()) {
    do {
      auto channel = dynamic_cast<apache::thrift::RocketClientChannel*>(it->second->getChannel());
      if (channel == nullptr || !channel->good()) {
        // Remove bad connection to create a new one.
        clientMap_->erase(it);
        VLOG(2) << "Invalid Channel: " << channel << " for host: " << host;
        break;
      }
      auto transport = dynamic_cast<folly::AsyncSocket*>(channel->getTransport());
      if (transport == nullptr || transport->hangup()) {
        clientMap_->erase(it);
        VLOG(2) << "Transport is closed by peers " << transport << " for host: " << host;
        break;
      }
      VLOG(2) << "Getting a client to " << host;
      return it->second;
    } while (false);
  }

  // Need to create a new client and insert it to client map.
  VLOG(2) << "There is no existing client to " << host << ", trying to create one";
  static thread_local int connectionCount = 0;
  /*
   * TODO(liuyu): folly said 'resolve' may take second to finish
   *              if this really happen, we will add a cache here.
   * */
  HostAddr resolved = host;
  if (!folly::IPAddress::validate(resolved.host)) {
    try {
      folly::SocketAddress socketAddr(resolved.host, resolved.port, true);
      std::ostringstream oss;
      oss << "resolve " << resolved << " as ";
      resolved.host = socketAddr.getAddressStr();
      oss << resolved;
      LOG(INFO) << oss.str();
    } catch (const std::exception& e) {
      // if we resolve failed, just return a connection, we will retry later
      LOG(ERROR) << e.what();
    }
  }

  VLOG(2) << "Connecting to " << host << " for " << ++connectionCount << " times";
  folly::AsyncTransport::UniquePtr socket;
  evb->runImmediatelyOrRunInEventBaseThreadAndWait([this, &socket, evb, resolved]() {
    if (enableSSL_) {
      auto sock = folly::AsyncSSLSocket::newSocket(nebula::createSSLContext(), evb);
      sock->connect(nullptr, resolved.host, resolved.port, FLAGS_conn_timeout_ms);
      socket = folly::AsyncTransport::UniquePtr(sock.release());
    } else {
      socket = folly::AsyncTransport::UniquePtr(
          new folly::AsyncSocket(evb, resolved.host, resolved.port, FLAGS_conn_timeout_ms));
    }
  });
  auto clientChannel = apache::thrift::RocketClientChannel::newChannel(std::move(socket));
  clientChannel->setTimeout(timeout);
  if (compatibility) {
    clientChannel->setProtocolId(apache::thrift::protocol::T_BINARY_PROTOCOL);
    //    clientChannel->setClientType(THRIFT_UNFRAMED_DEPRECATED);
  }
  std::shared_ptr<ClientType> client(new ClientType(std::move(clientChannel)), [evb](auto* p) {
    evb->runImmediatelyOrRunInEventBaseThreadAndWait([p] { delete p; });
  });
  clientMap_->emplace(std::make_pair(host, evb), client);
  return client;
}

}  // namespace thrift
}  // namespace nebula
#endif
