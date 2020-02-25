/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/Common.h"
#include "meta/MetaHttpAdminHandler.h"
#include "webservice/Common.h"
#include "webservice/WebService.h"
#include "process/ProcessUtils.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

namespace nebula {
namespace meta {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void MetaHttpAdminHandler::init(nebula::kvstore::KVStore *kvstore) {
    kvstore_ = kvstore;
    CHECK_NOTNULL(kvstore_);
}

void MetaHttpAdminHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    LOG(INFO) << __PRETTY_FUNCTION__;
    if (headers->getMethod().value() != HTTPMethod::GET) {
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }
    if (!headers->hasQueryParam("op")) {
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }
    if (kvstore_ == nullptr) {
        err_ = HttpCode::SUCCEEDED;
        resp_ = folly::stringPrintf("No kvstore");
        return;
    }

    err_ = HttpCode::SUCCEEDED;
    auto* op = headers->getQueryParamPtr("op");
    if (*op == "compact") {
        LOG(INFO) << "do compact at default space";
        auto status = kvstore_->compact(kDefaultSpaceId);
        if (status != kvstore::ResultCode::SUCCEEDED) {
            resp_ = folly::stringPrintf("Admin failed! error=%d", static_cast<int32_t>(status));
            return;
        }
    } else if (*op == "flush") {
        auto status = kvstore_->flush(kDefaultSpaceId);
        if (status != kvstore::ResultCode::SUCCEEDED) {
            resp_ = folly::stringPrintf("Flush failed! error=%d", static_cast<int32_t>(status));
            return;
        }
    } else {
        resp_ = folly::stringPrintf("Unknown operation %s", op->c_str());
        return;
    }

    resp_ = "ok";
}

void MetaHttpAdminHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}

void MetaHttpAdminHandler::onEOM() noexcept {
    switch (err_) {
        case HttpCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                        WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
                .body(WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
                .sendWithEOM();
            return;
        case HttpCode::E_ILLEGAL_ARGUMENT:
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::BAD_REQUEST),
                        WebServiceUtils::toString(HttpStatusCode::BAD_REQUEST))
                .body(WebServiceUtils::toString(HttpStatusCode::BAD_REQUEST))
                .sendWithEOM();
            return;
        default:
            break;
    }

    ResponseBuilder(downstream_)
        .status(WebServiceUtils::to(HttpStatusCode::OK),
                WebServiceUtils::toString(HttpStatusCode::OK))
        .body(resp_)
        .sendWithEOM();
}

void MetaHttpAdminHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void MetaHttpAdminHandler::requestComplete() noexcept {
    delete this;
}


void MetaHttpAdminHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web Service MetaHttpAdminHandler got error : "
               << proxygen::getErrorString(error);
}

}  // namespace meta
}  // namespace nebula
