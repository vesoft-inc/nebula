/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef COMMON_THRIFT_THRIFTLOCALCLIENTMANAGER_H
#define COMMON_THRIFT_THRIFTLOCALCLIENTMANAGER_H

#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBaseManager.h>

#include "common/base/Base.h"
#include "common/datatypes/HostAddr.h"
namespace nebula {
namespace thrift {

template <class ClientType>
class LocalClientManager final {
 public:
  std::shared_ptr<ClientType> client(const HostAddr& host,
                                     folly::EventBase* evb = nullptr,
                                     bool compatibility = false,
                                     uint32_t timeout = 0) {
    UNUSED(host);
    UNUSED(evb);
    UNUSED(compatibility);
    UNUSED(timeout);
    return ClientType::getInstance();
  }

  ~LocalClientManager() {
    VLOG(3) << "~LocalClientManager";
  }

  explicit LocalClientManager(bool enableSSL = false) {
    UNUSED(enableSSL);
    VLOG(3) << "LocalClientManager";
  }
};
}  // namespace thrift
}  // namespace nebula
#endif
