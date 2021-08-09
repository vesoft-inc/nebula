/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/webservice/GetStatsHandler.h"
#include "common/webservice/Common.h"
#include "common/stats/StatsManager.h"
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

    if (headers->hasQueryParam("format")) {
        returnJson_ = (headers->getQueryParam("format") == "json");
    }

    if (headers->hasQueryParam("stats")) {
        const std::string& stats = headers->getQueryParam("stats");
        folly::split(",", stats, statNames_, true);
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

void GetStatsHandler::addOneStat(folly::dynamic& vals,
                                 const std::string& statName,
                                 int64_t statValue) const {
    vals.push_back(folly::dynamic::object(statName, statValue));
}

void GetStatsHandler::addOneStat(folly::dynamic& vals,
                                 const std::string& statName,
                                 const std::string& error) const {
    vals.push_back(folly::dynamic::object(statName, error));
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
                int64_t statValue = status.value();
                addOneStat(stats, sn, statValue);
            } else {
                addOneStat(stats, sn, status.status().toString());
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
