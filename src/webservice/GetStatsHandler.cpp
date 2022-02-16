/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "webservice/GetStatsHandler.h"

#include <folly/Optional.h>                       // for Optional
#include <folly/String.h>                         // for split
#include <folly/detail/Iterators.h>               // for operator!=, Iterato...
#include <folly/dynamic.h>                        // for dynamic::dynamic
#include <folly/json.h>                           // for toPrettyJson
#include <proxygen/httpserver/ResponseBuilder.h>  // for ResponseBuilder
#include <proxygen/lib/http/HTTPMessage.h>        // for HTTPMessage
#include <proxygen/lib/http/HTTPMethod.h>         // for HTTPMethod, HTTPMet...
#include <proxygen/lib/http/ProxygenErrorEnum.h>  // for getErrorString, Pro...

#include <ostream>      // for operator<<, basic_o...
#include <type_traits>  // for remove_reference<>:...
#include <utility>      // for move, pair

#include "common/base/Logging.h"        // for LOG, LogMessage
#include "common/base/Status.h"         // for Status
#include "common/base/StatusOr.h"       // for StatusOr
#include "common/stats/StatsManager.h"  // for StatsManager
#include "webservice/Common.h"          // for HttpStatusCode, Web...

namespace folly {
class IOBuf;

class IOBuf;
}  // namespace folly

namespace nebula {

using nebula::stats::StatsManager;
using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::ResponseBuilder;
using proxygen::UpgradeProtocol;

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
  LOG(ERROR) << "Web service GetStatsHandler got error: " << proxygen::getErrorString(err);
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
