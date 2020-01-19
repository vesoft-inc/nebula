/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "webservice/SetFlagsHandler.h"
#include "webservice/Common.h"
#include <folly/Conv.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

namespace nebula {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void SetFlagsHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::PUT) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }
}

void SetFlagsHandler::onBody(std::unique_ptr<folly::IOBuf> body) noexcept {
    try {
        std::string str = body->moveToFbString().toStdString();
        flags_ = folly::parseJson(str);
    } catch (const std::exception &e) {
        err_ = HttpCode::E_UNPROCESSABLE;
    }
}

void SetFlagsHandler::onEOM() noexcept {
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
    for (auto &item : flags_.items()) {
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
    LOG(ERROR) << "Web service SetFlagsHandler got error: "
               << proxygen::getErrorString(err);
    delete this;
}

}  // namespace nebula
