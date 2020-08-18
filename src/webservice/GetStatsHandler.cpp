/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "webservice/GetStatsHandler.h"
#include "webservice/Common.h"
#include "stats/StatsManager.h"
#include "thread/ThreadManager.h"
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
                .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                        WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
                .sendWithEOM();
            return;
        default:
            break;
    }

    // read stats
    folly::dynamic vals = getStats();
    if (returnJson_) {
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::OK),
                    WebServiceUtils::toString(HttpStatusCode::OK))
            .body(folly::toJson(vals))
            .sendWithEOM();
    } else {
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::OK),
                    WebServiceUtils::toString(HttpStatusCode::OK))
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
                                 int64_t statValue) const {
    folly::dynamic stat = folly::dynamic::object();
    stat["name"] = statName;
    stat["value"] = statValue;
    vals.push_back(std::move(stat));
}


void GetStatsHandler::addOneStat(folly::dynamic& vals,
                                 const std::string& statName,
                                 const std::string& error) const {
    folly::dynamic stat = folly::dynamic::object();
    stat["name"] = statName;
    stat["value"] = error;
    vals.push_back(std::move(stat));
}


folly::dynamic GetStatsHandler::getStats() const {
    auto stats = folly::dynamic::array();
    static std::unordered_map<std::string, std::function<size_t()>> funcMap =
            getFunctionMap();
    if (statNames_.empty()) {
        // Read all stats
        StatsManager::readAllValue(stats);
        for (auto& func : funcMap) {
            addOneStat(stats, func.first, func.second());
        }
    } else {
        for (auto& sn : statNames_) {
            if (funcMap.find(sn) != funcMap.end()) {
                addOneStat(stats, sn, funcMap[sn]());
                continue;
            }
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

std::unordered_map<std::string, std::function<size_t()>>
        GetStatsHandler::getFunctionMap() const {
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> tm =
            nebula::thread::getThreadManager();
    std::unordered_map<std::string, std::function<size_t()>> funcMap = {
        { "rpc.numIdleWorkers", [tm]() -> auto { return tm->idleWorkerCount();} },
        { "rpc.pendingTaskCount", [tm]() -> auto { return tm->pendingTaskCount();} },
        { "rpc.totalTaskCount", [tm]() -> auto { return tm->totalTaskCount();} },
        { "rpc.expiredTaskCount", [tm]() -> auto { return tm->expiredTaskCount();} }
    };
    return funcMap;
}

std::string GetStatsHandler::toStr(folly::dynamic& vals) const {
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
