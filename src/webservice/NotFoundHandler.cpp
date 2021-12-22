/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "webservice/NotFoundHandler.h"

#include <proxygen/httpserver/ResponseBuilder.h>

#include "common/base/Base.h"
#include "webservice/Common.h"

namespace nebula {

using proxygen::HTTPMessage;
using proxygen::ProxygenError;
using proxygen::ResponseBuilder;
using proxygen::UpgradeProtocol;

void NotFoundHandler::onRequest(std::unique_ptr<HTTPMessage>) noexcept {
  // Do nothing
}

void NotFoundHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
  // Do nothing, we only support GET
}

void NotFoundHandler::onEOM() noexcept {
  ResponseBuilder(downstream_)
      .status(WebServiceUtils::to(HttpStatusCode::NOT_FOUND),
              WebServiceUtils::toString(HttpStatusCode::NOT_FOUND))
      .sendWithEOM();
  return;
}

void NotFoundHandler::onUpgrade(UpgradeProtocol) noexcept {
  // Do nothing
}

void NotFoundHandler::requestComplete() noexcept {
  delete this;
}

void NotFoundHandler::onError(ProxygenError err) noexcept {
  LOG(ERROR) << "Web service NotFoundHandler got error: " << proxygen::getErrorString(err);
  delete this;
}

}  // namespace nebula
