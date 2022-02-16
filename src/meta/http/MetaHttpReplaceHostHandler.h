/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_HTTP_METAHTTPREPLACEHOSTHANDLER_H
#define META_HTTP_METAHTTPREPLACEHOSTHANDLER_H

#include <gtest/gtest_prod.h>                     // for FRIEND_TEST
#include <proxygen/httpserver/RequestHandler.h>   // for RequestHandler
#include <proxygen/lib/http/HTTPConstants.h>      // for UpgradeProtocol
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for ProxygenError

#include <memory>  // for unique_ptr
#include <string>  // for string

#include "common/base/Base.h"
#include "common/thread/GenericThreadPool.h"
#include "kvstore/KVStore.h"
#include "webservice/Common.h"  // for HttpCode, HttpCode:...

namespace nebula {
namespace kvstore {
class KVStore;
}  // namespace kvstore
}  // namespace nebula
namespace proxygen {
class HTTPMessage;
}  // namespace proxygen

namespace folly {
class IOBuf;

class IOBuf;
}  // namespace folly
namespace proxygen {
class HTTPMessage;
}  // namespace proxygen

namespace nebula {
namespace kvstore {
class KVStore;
}  // namespace kvstore

namespace meta {

using nebula::HttpCode;

/**
 * @brief It will replace host info in meta partition table from
 *        backup host to current cluster host.
 *        It will replace given host in zone table and partition table. Notice that,
 *        it only replace the host without port.
 *        Functions such as onRequest, onBody... and requestComplete are inherited
 *        from RequestHandler, we will check request parameters in onRequest and
 *        call main logic in onEOM.
 *
 */
class MetaHttpReplaceHostHandler : public proxygen::RequestHandler {
  FRIEND_TEST(MetaHttpReplaceHandlerTest, FooTest);

 public:
  MetaHttpReplaceHostHandler() = default;

  void init(nebula::kvstore::KVStore* kvstore);

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError error) noexcept override;

  bool replaceHostInPart(std::string ipv4From, std::string ipv4To);
  bool replaceHostInZone(std::string ipv4From, std::string ipv4To);

 private:
  HttpCode err_{HttpCode::SUCCEEDED};
  std::string errMsg_;
  std::string ipv4From_;
  std::string ipv4To_;
  nebula::kvstore::KVStore* kvstore_;
};

}  // namespace meta
}  // namespace nebula
#endif
