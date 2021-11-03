/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef WEBSERVICE_NOTFOUNDHANDLER_H_
#define WEBSERVICE_NOTFOUNDHANDLER_H_

#include <proxygen/httpserver/RequestHandler.h>

#include "common/base/Base.h"

namespace nebula {

class NotFoundHandler : public proxygen::RequestHandler {
 public:
  NotFoundHandler() = default;

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError err) noexcept override;
};

}  // namespace nebula
#endif  // WEBSERVICE_NOTFOUNDHANDLER_H_
