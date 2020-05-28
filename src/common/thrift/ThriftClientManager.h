/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_THRIFT_THRIFTCLIENTMANAGER_H_
#define COMMON_THRIFT_THRIFTCLIENTMANAGER_H_

#include "common/base/Base.h"
#include <folly/io/async/EventBaseManager.h>
#include "common/datatypes/HostAddr.h"

namespace nebula {
namespace thrift {

template<class ClientType>
class ThriftClientManager final {
public:
    std::shared_ptr<ClientType> client(const HostAddr& host,
                                       folly::EventBase* evb = nullptr,
                                       bool compatibility = false,
                                       uint32_t timeout = 0);

    ~ThriftClientManager() {
        VLOG(3) << "~ThriftClientManager";
    }

    ThriftClientManager() {
        VLOG(3) << "ThriftClientManager";
    }

private:
    using ClientMap = std::unordered_map<
        std::pair<HostAddr, folly::EventBase*>,     // <ip, port> pair
        std::shared_ptr<ClientType>                 // Async thrift client
    >;

    folly::ThreadLocal<ClientMap> clientMap_;
};

}  // namespace thrift
}  // namespace nebula

#include "common/thrift/ThriftClientManager.inl"

#endif  // COMMON_THRIFT_THRIFTCLIENTMANAGER_H_
