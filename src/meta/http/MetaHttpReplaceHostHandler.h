/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_HTTP_METAHTTPREPLACEHOSTHANDLER_H
#define META_HTTP_METAHTTPREPLACEHOSTHANDLER_H

#include <proxygen/httpserver/RequestHandler.h>

#include "common/base/Base.h"
#include "common/thread/GenericThreadPool.h"
#include "kvstore/KVStore.h"
#include "webservice/Common.h"

namespace nebula {
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
