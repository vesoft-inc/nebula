/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_THRIFT_THRIFTCLIENTMANAGER_H_
#define COMMON_THRIFT_THRIFTCLIENTMANAGER_H_

#include "base/Base.h"
#include <folly/io/async/EventBaseManager.h>

namespace nebula {
namespace thrift {

template<class ClientType>
class ThriftClientManager final {
public:
    std::shared_ptr<ClientType> client(const HostAddr& host, folly::EventBase* evb = nullptr);

    ~ThriftClientManager() {
        VLOG(3) << "~ThriftClientManager";
    }

    ThriftClientManager() {
        VLOG(3) << "ThriftClientManager";
    }

    // Destroy all clients for all threads
    void destroyAll();

private:
    using ClientMap = std::unordered_map<
        std::pair<HostAddr, folly::EventBase*>,     // <ip, port> pair
        std::shared_ptr<ClientType>                 // Async thrift client
    >;

    struct DumyTag;  // Required by `ThreadLocal::Accessor' if we invoke `accessAllThreads'
    folly::ThreadLocal<ClientMap, DumyTag> clientMap_;
};

}  // namespace thrift
}  // namespace nebula

#include "thrift/ThriftClientManager.inl"

#endif  // COMMON_THRIFT_THRIFTCLIENTMANAGER_H_

