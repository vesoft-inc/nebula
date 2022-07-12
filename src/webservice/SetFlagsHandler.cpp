/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "webservice/SetFlagsHandler.h"

#include <folly/Conv.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>

#include "common/base/Base.h"
#include "webservice/Common.h"

namespace nebula {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::ResponseBuilder;
using proxygen::UpgradeProtocol;

void SetFlagsHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
  if (!headers->getMethod() || headers->getMethod().value() != HTTPMethod::PUT) {
    // Unsupported method
    err_ = HttpCode::E_UNSUPPORTED_METHOD;
    return;
  }
}

void SetFlagsHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
  if (body_) {
    body_->appendChain(std::move(body));
  } else {
    body_ = std::move(body);
  }
}

void SetFlagsHandler::onEOM() noexcept {
  folly::dynamic flags;
  try {
    if (!body_) {
      LOG(ERROR) << "Got an empty body";
      err_ = HttpCode::E_UNPROCESSABLE;
    } else {
      std::string body = body_->moveToFbString().toStdString();
      flags = folly::parseJson(body);
      if (flags.empty()) {
        err_ = HttpCode::E_UNPROCESSABLE;
      }
    }
  } catch (const std::exception &e) {
    LOG(ERROR) << "Fail to update flags: " << e.what();
    err_ = HttpCode::E_UNPROCESSABLE;
  }
  switch (err_) {
    case HttpCode::E_UNSUPPORTED_METHOD:
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                  WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
          .sendWithEOM();
      return;
    case HttpCode::E_UNPROCESSABLE:
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::BAD_REQUEST),
                  WebServiceUtils::toString(HttpStatusCode::BAD_REQUEST))
          .sendWithEOM();
      return;
    default:
      break;
  }

  folly::dynamic failedOptions = folly::dynamic::array();
  for (auto &item : flags.items()) {
    const std::string &name = item.first.asString();
    const std::string &value = item.second.asString();
    const std::string &newValue = gflags::SetCommandLineOption(name.c_str(), value.c_str());
    if (newValue.empty()) {
      failedOptions.push_back(name);
    }
  }
  folly::dynamic body = failedOptions.empty()
                            ? folly::dynamic::object("errCode", 0)
                            : folly::dynamic::object("failedOptions", failedOptions);
  ResponseBuilder(downstream_)
      .status(WebServiceUtils::to(HttpStatusCode::OK),
              WebServiceUtils::toString(HttpStatusCode::OK))
      .body(folly::toPrettyJson(body))
      .sendWithEOM();
}

void SetFlagsHandler::onUpgrade(UpgradeProtocol) noexcept {
  // Do nothing
}

void SetFlagsHandler::requestComplete() noexcept {
  delete this;
}

void SetFlagsHandler::onError(ProxygenError err) noexcept {
  LOG(ERROR) << "Web service SetFlagsHandler got error: " << proxygen::getErrorString(err);
  delete this;
}

}  // namespace nebula
