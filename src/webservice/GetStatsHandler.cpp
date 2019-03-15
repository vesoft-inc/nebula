/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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

    if (headers->getQueryParamPtr("returnjson") != nullptr) {
        returnJson_ = true;
    }

    auto* statsStr = headers->getQueryParamPtr("stats");
    if (statsStr != nullptr) {
        folly::split(",", *statsStr, statNames_, true);
    }
}


void GetStatsHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void GetStatsHandler::onEOM() noexcept {
    switch (err_) {
        case HttpCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(405, "Method Not Allowed")
                .sendWithEOM();
            return;
        default:
            break;
    }

    // read stats
    folly::dynamic vals = getStats();
    if (returnJson_) {
        ResponseBuilder(downstream_)
            .status(200, "OK")
            .body(folly::toJson(vals))
            .sendWithEOM();
    } else {
        ResponseBuilder(downstream_)
            .status(200, "OK")
            .body(toStr(vals))
            .sendWithEOM();
    }
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


void GetStatsHandler::addOneStat(folly::dynamic& vals,
                                 const std::string& statName,
                                 int64_t statValue) {
    folly::dynamic stat = folly::dynamic::object();
    stat["name"] = statName;
    stat["value"] = statValue;
    vals.push_back(std::move(stat));
}


folly::dynamic GetStatsHandler::getStats() {
    auto stats = folly::dynamic::array();
    if (statNames_.empty()) {
        // Read all stats
        StatsManager::readAllValue(stats);
    } else {
        for (auto& sn : statNames_) {
            int64_t statValue = StatsManager::readValue(sn);
            addOneStat(stats, sn, statValue);
        }
    }

    return stats;
}


std::string GetStatsHandler::toStr(folly::dynamic& vals) {
    std::stringstream ss;
    for (auto& counter : vals) {
        auto& val = counter["value"];
        ss << counter["name"].asString() << "="
           << val.asString()
           << "\n";
    }
    return ss.str();
}

}  // namespace nebula
