/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_HTTP_METAHTTPINGESTHANDLER_H
#define META_HTTP_METAHTTPINGESTHANDLER_H

#include <proxygen/httpserver/RequestHandler.h>

#include "common/base/Base.h"
#include "common/thread/GenericThreadPool.h"
#include "kvstore/KVStore.h"
#include "webservice/Common.h"

namespace nebula {
namespace meta {

using nebula::HttpCode;

/**
 * @brief Ingest should be called after download successfully.
 *        It will instruct relative storaged to ingest sst files
 *        from local download folder by sending http request.
 *        It will handle one space each time.
 *        Functions such as onRequest, onBody... and requestComplete are inherited
 *        from RequestHandler, we will check request parameters in onRequest and
 *        call main logic in onEOM.
 *
 */
class MetaHttpIngestHandler : public proxygen::RequestHandler {
 public:
  MetaHttpIngestHandler() = default;

  void init(nebula::kvstore::KVStore *kvstore, nebula::thread::GenericThreadPool *pool);

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError error) noexcept override;

  bool ingestSSTFiles(GraphSpaceID space);

 private:
  HttpCode err_{HttpCode::SUCCEEDED};
  GraphSpaceID space_;
  nebula::kvstore::KVStore *kvstore_;
  nebula::thread::GenericThreadPool *pool_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_HTTP_METAHTTPINGESTHANDLER_H
