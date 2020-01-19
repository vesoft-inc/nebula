/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "webservice/GetStatsHandler.h"
#include "webservice/Common.h"
#include "stats/StatsManager.h"
#include <folly/String.h>
#include <folly/json.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

namespace nebula {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;
using nebula::stats::StatsManager;

void GetStatsHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (headers->hasQueryParam("return")) {
        returnJson_ = (headers->getQueryParam("return") == "json");
    }

    if (headers->hasQueryParam("names")) {
        const std::string& names = headers->getQueryParam("names");
        folly::split(",", names, statNames_, true);
    }
}


void GetStatsHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void GetStatsHandler::onEOM() noexcept {
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

    // read stats
    folly::dynamic vals = getStats();
    std::string body = returnJson_ ? folly::toPrettyJson(vals) : toStr(vals);
    ResponseBuilder(downstream_)
        .status(WebServiceUtils::to(HttpStatusCode::OK),
                WebServiceUtils::toString(HttpStatusCode::OK))
        .body(std::move(body))
        .sendWithEOM();
}


void GetStatsHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void GetStatsHandler::requestComplete() noexcept {
    delete this;
}


void GetStatsHandler::onError(ProxygenError err) noexcept {
    LOG(ERROR) << "Web service GetStatsHandler got error: "
               << proxygen::getErrorString(err);
    delete this;
}

folly::dynamic GetStatsHandler::getStats() const {
    auto stats = folly::dynamic::array();
    if (statNames_.empty()) {
        // Read all stats
        StatsManager::readAllValue(stats);
    } else {
        for (auto& sn : statNames_) {
            auto status = StatsManager::readValue(sn);
            if (status.ok()) {
                stats.push_back(folly::dynamic::object(sn, status.value()));
            } else {
                stats.push_back(folly::dynamic::object(sn, status.status().toString()));
            }
        }
    }

    return stats;
}


std::string GetStatsHandler::toStr(folly::dynamic& vals) const {
    std::stringstream ss;
    for (auto& counter : vals) {
        for (auto& m : counter.items()) {
            ss << m.first.asString() << "=" << m.second.asString() << "\n";
        }
    }
    return ss.str();
}

}  // namespace nebula
