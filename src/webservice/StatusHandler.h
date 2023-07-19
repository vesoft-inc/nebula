/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef WEBSERVICE_STATUSHANDLER_H_
#define WEBSERVICE_STATUSHANDLER_H_

#include <proxygen/httpserver/RequestHandler.h>

#include "common/base/Base.h"
#include "webservice/Common.h"

namespace nebula {

using nebula::HttpCode;

class StatusHandler : public proxygen::RequestHandler {
 public:
  StatusHandler() = default;

  void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

  void onEOM() noexcept override;

  void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

  void requestComplete() noexcept override;

  void onError(proxygen::ProxygenError error) noexcept override;

 private:
  folly::dynamic getStatus();

 private:
  HttpCode err_{HttpCode::SUCCEEDED};
  std::vector<std::string> statusNames_;
  std::vector<std::string> statusAllNames_{"status"};
};

}  // namespace nebula

#endif  // WEBSERVICE_STATUSHANDLER_H_
