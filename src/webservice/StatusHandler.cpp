/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "webservice/StatusHandler.h"

#include <folly/Optional.h>                       // for Optional
#include <folly/dynamic.h>                        // for dynamic::dynamic
#include <folly/json.h>                           // for toJson
#include <proxygen/httpserver/ResponseBuilder.h>  // for HTTPMethod, HTTPMet...
#include <proxygen/lib/http/HTTPMessage.h>        // for HTTPMessage
#include <proxygen/lib/http/HTTPMethod.h>         // for HTTPMethod, HTTPMet...
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for getErrorString, Pro...

#include <ostream>  // for operator<<, basic_o...

#include "common/base/Logging.h"  // for LOG, LogMessage
#include "version/Version.h"      // for gitInfoSha

namespace folly {
class IOBuf;

class IOBuf;
}  // namespace folly

namespace nebula {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::ResponseBuilder;
using proxygen::UpgradeProtocol;

void StatusHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
  if (headers->getMethod().value() != HTTPMethod::GET) {
    // Unsupported method
    err_ = HttpCode::E_UNSUPPORTED_METHOD;
    return;
  }
}

void StatusHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
  // Do nothing, we only support GET
}

void StatusHandler::onEOM() noexcept {
  switch (err_) {
    case HttpCode::E_UNSUPPORTED_METHOD:
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                  WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
          .sendWithEOM();
      return;
    default:
      break;
  }

  folly::dynamic vals = getStatus();
  ResponseBuilder(downstream_)
      .status(WebServiceUtils::to(HttpStatusCode::OK),
              WebServiceUtils::toString(HttpStatusCode::OK))
      .body(folly::toJson(vals))
      .sendWithEOM();
}

void StatusHandler::onUpgrade(UpgradeProtocol) noexcept {
  // Do nothing
}

void StatusHandler::requestComplete() noexcept {
  delete this;
}

void StatusHandler::onError(ProxygenError error) noexcept {
  LOG(ERROR) << "Web service StorageHttpHandler got error: " << proxygen::getErrorString(error);
}

folly::dynamic StatusHandler::getStatus() {
  folly::dynamic json = folly::dynamic::object();
  json["status"] = "running";
  json["git_info_sha"] = gitInfoSha();
  return json;
}

}  // namespace nebula
