/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "webservice/SetFlagsHandler.h"
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
        err_ = ErrorCode::E_UNSUPPORTED_METHOD;
        return;
    }

    VLOG(1) << "SetFlagsHandler got query string \"" << headers->getQueryString();

    if (headers->hasQueryParam("flag")) {
        name_ = headers->getDecodedQueryParam("flag");
    } else {
        LOG(ERROR) << "There is no flag specified when setting the flag value";
        err_ = ErrorCode::E_UNPROCESSABLE;
    }

    if (headers->hasQueryParam("value")) {
        value_ = headers->getDecodedQueryParam("value");
    } else {
        LOG(ERROR) << "There is no value specified when setting the flag value";
        err_ = ErrorCode::E_UNPROCESSABLE;
    }

    if (err_ == ErrorCode::SUCCEEDED) {
        VLOG(1) << "Set flag " << name_ << " = " << value_;
    }
}


void SetFlagsHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void SetFlagsHandler::onEOM() noexcept {
    switch (err_) {
        case ErrorCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(405, "Method Not Allowed")
                .sendWithEOM();
            return;
        case ErrorCode::E_UNPROCESSABLE:
            ResponseBuilder(downstream_)
                .status(400, "Bad Request")
                .sendWithEOM();
            return;
        default:
            break;
    }

    if (gflags::SetCommandLineOption(name_.c_str(), value_.c_str()).empty()) {
        // Failed
        ResponseBuilder(downstream_)
            .status(200, "OK")
            .body("false")
            .sendWithEOM();
    } else {
        ResponseBuilder(downstream_)
            .status(200, "OK")
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
