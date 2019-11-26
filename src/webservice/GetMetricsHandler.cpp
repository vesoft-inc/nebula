/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "webservice/GetStatsHandler.h"
#include "webservice/Common.h"
#include "stats/StatsManager.h"
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include "GetMetricsHandler.h"

namespace nebula {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;
using nebula::stats::StatsManager;

void GetMetricsHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }
    serializer_ = DCHECK_NOTNULL(dynamic_cast<stats::MetricsSerializer*>(&StatsManager::get()));
    if (serializer_ == nullptr) {
        err_ = HttpCode::E_NULL_POINTER;
    }
}


void GetMetricsHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void GetMetricsHandler::onEOM() noexcept {
    switch (err_) {
        case HttpCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                        WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
                .sendWithEOM();
            return;
        case HttpCode::E_NULL_POINTER:
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::INTERNAL_ERROR),
                        WebServiceUtils::toString(HttpStatusCode::INTERNAL_ERROR))
                .sendWithEOM();
            return;
        default:
            break;
    }

    // read metrics
    std::string vals = DCHECK_NOTNULL(serializer_)->serialize();
    ResponseBuilder(downstream_)
        .status(WebServiceUtils::to(HttpStatusCode::OK),
                WebServiceUtils::toString(HttpStatusCode::OK))
        .body(vals)
        .sendWithEOM();
}


void GetMetricsHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void GetMetricsHandler::requestComplete() noexcept {
    delete this;
}


void GetMetricsHandler::onError(ProxygenError err) noexcept {
    LOG(ERROR) << "Web service GetMetricsHandler got error: "
               << proxygen::getErrorString(err);
    delete this;
}

}  // namespace nebula

