/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_NETWORK_THRIFTSOCKETMANAGER_H_
#define COMMON_NETWORK_THRIFTSOCKETMANAGER_H_

#include "base/Base.h"
#include <thrift/lib/cpp/async/TAsyncSocket.h>

namespace vesoft {
namespace network {

class ThriftSocketManager final {
public:
    ThriftSocketManager() = delete;

    static std::shared_ptr<apache::thrift::async::TAsyncSocket> getSocket(
        const HostAddr& host);

    static void disconnectOnThread();

private:
    using SocketMap = std::unordered_map<
        HostAddr,  // <ip, port> pair
        std::shared_ptr<apache::thrift::async::TAsyncSocket>  // Async socket
    >;

    static folly::ThreadLocal<SocketMap> socketMap_;
};

}  // namespace network
}  // namespace vesoft
#endif  // COMMON_NETWORK_THRIFTSOCKETMANAGER_H_

