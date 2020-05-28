/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/webservice/SetFlagsHandler.h"
#include "common/webservice/Common.h"
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
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    VLOG(1) << "SetFlagsHandler got query string \"" << headers->getQueryString();

    if (headers->hasQueryParam("flag")) {
        name_ = headers->getDecodedQueryParam("flag");
    } else {
        LOG(ERROR) << "There is no flag specified when setting the flag value";
        err_ = HttpCode::E_UNPROCESSABLE;
    }

    if (headers->hasQueryParam("value")) {
        value_ = headers->getDecodedQueryParam("value");
    } else {
        LOG(ERROR) << "There is no value specified when setting the flag value";
        err_ = HttpCode::E_UNPROCESSABLE;
    }

    if (err_ == HttpCode::SUCCEEDED) {
        VLOG(1) << "Set flag " << name_ << " = " << value_;
    }
}


void SetFlagsHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
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

    if (gflags::SetCommandLineOption(name_.c_str(), value_.c_str()).empty()) {
        // Failed
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::OK),
                    WebServiceUtils::toString(HttpStatusCode::OK))
            .body("false")
            .sendWithEOM();
    } else {
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::OK),
                    WebServiceUtils::toString(HttpStatusCode::OK))
            .body("true")
            .sendWithEOM();
    }
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
