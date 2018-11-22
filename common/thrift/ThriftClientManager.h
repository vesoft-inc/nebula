/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_THRIFT_THRIFTCLIENTMANAGER_H_
#define COMMON_THRIFT_THRIFTCLIENTMANAGER_H_

#include "base/Base.h"

namespace vesoft {
namespace thrift {

template<class ClientType>
class ThriftClientManager final {
public:
    // Get the thrift client for the current thread
    static std::shared_ptr<ClientType> getClient(const HostAddr& host);

private:
    ThriftClientManager() = default;

private:
    using ClientMap = std::unordered_map<
        HostAddr,  // <ip, port> pair
        std::shared_ptr<ClientType>  // Async thrift client
    >;

    folly::ThreadLocal<ClientMap> clientMap_;
};

}  // namespace thrift
}  // namespace vesoft

#include "thrift/ThriftClientManager.inl"

#endif  // COMMON_THRIFT_THRIFTCLIENTMANAGER_H_

