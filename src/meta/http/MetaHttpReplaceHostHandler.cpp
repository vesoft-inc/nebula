/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/http/MetaHttpReplaceHostHandler.h"

#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>

#include "common/http/HttpClient.h"
#include "common/network/NetworkUtils.h"
#include "common/process/ProcessUtils.h"
#include "common/thread/GenericThreadPool.h"
#include "common/utils/MetaKeyUtils.h"
#include "meta/processors/Common.h"
#include "webservice/Common.h"
#include "webservice/WebService.h"

namespace nebula {
namespace meta {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::ResponseBuilder;
using proxygen::UpgradeProtocol;

void MetaHttpReplaceHostHandler::init(nebula::kvstore::KVStore* kvstore) {
  kvstore_ = kvstore;
  CHECK_NOTNULL(kvstore_);
}

void MetaHttpReplaceHostHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
  LOG(INFO) << __PRETTY_FUNCTION__;

  if (!headers->hasQueryParam("from")) {
    err_ = HttpCode::E_ILLEGAL_ARGUMENT;
    errMsg_ = "Miss argument [from]";
    return;
  }

  if (!headers->hasQueryParam("to")) {
    err_ = HttpCode::E_ILLEGAL_ARGUMENT;
    errMsg_ = "Miss argument [to]";
    return;
  }

  ipv4From_ = headers->getQueryParam("from");
  ipv4To_ = headers->getQueryParam("to");

  LOG(INFO) << folly::sformat("Change host info from {} to {}", ipv4From_, ipv4To_);
}

void MetaHttpReplaceHostHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
  // Do nothing, we only support GET
}

void MetaHttpReplaceHostHandler::onEOM() noexcept {
  switch (err_) {
    case HttpCode::E_UNSUPPORTED_METHOD:
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                  WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
          .sendWithEOM();
      return;
    case HttpCode::E_ILLEGAL_ARGUMENT:
      LOG(INFO) << errMsg_;
      ResponseBuilder(downstream_)
          .status(WebServiceUtils::to(HttpStatusCode::BAD_REQUEST), errMsg_)
          .sendWithEOM();
      return;
    default:
      break;
  }

  if (replaceHostInPart(ipv4From_, ipv4To_) && replaceHostInZone(ipv4From_, ipv4To_)) {
    LOG(INFO) << "Replace Host in partition and zone successfully";
    ResponseBuilder(downstream_)
        .status(WebServiceUtils::to(HttpStatusCode::OK),
                WebServiceUtils::toString(HttpStatusCode::OK))
        .body("Replace Host in partition and zone successfully")
        .sendWithEOM();
  } else {
    LOG(INFO) << "Replace Host in partition and zone failed";
    ResponseBuilder(downstream_)
        .status(WebServiceUtils::to(HttpStatusCode::FORBIDDEN),
                WebServiceUtils::toString(HttpStatusCode::FORBIDDEN))
        .body(errMsg_)
        .sendWithEOM();
  }
}

void MetaHttpReplaceHostHandler::onUpgrade(UpgradeProtocol) noexcept {
  // Do nothing
}

void MetaHttpReplaceHostHandler::requestComplete() noexcept {
  delete this;
}

void MetaHttpReplaceHostHandler::onError(ProxygenError error) noexcept {
  LOG(INFO) << "Web Service MetaHttpReplaceHostHandler got error : "
            << proxygen::getErrorString(error);
}

bool MetaHttpReplaceHostHandler::replaceHostInPart(std::string ipv4From, std::string ipv4To) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  const auto& spacePrefix = MetaKeyUtils::spacePrefix();
  std::unique_ptr<kvstore::KVIterator> iter;
  auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, spacePrefix, &iter);
  if (kvRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    errMsg_ = folly::stringPrintf("Can't get space prefix=%s", spacePrefix.c_str());
    LOG(INFO) << errMsg_;
    return false;
  }

  std::vector<GraphSpaceID> allSpaceId;
  while (iter->valid()) {
    auto spaceId = MetaKeyUtils::spaceId(iter->key());
    allSpaceId.emplace_back(spaceId);
    iter->next();
  }
  LOG(INFO) << "AllSpaceId.size()=" << allSpaceId.size();

  std::vector<nebula::kvstore::KV> data;
  for (const auto& spaceId : allSpaceId) {
    const auto& partPrefix = MetaKeyUtils::partPrefix(spaceId);
    kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
    if (kvRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
      errMsg_ = folly::stringPrintf("Can't get partPrefix=%s", partPrefix.c_str());
      LOG(INFO) << errMsg_;
      return false;
    }

    while (iter->valid()) {
      bool needUpdate = false;
      auto partHosts = MetaKeyUtils::parsePartVal(iter->val());
      for (auto& host : partHosts) {
        if (host.host == ipv4From) {
          needUpdate = true;
          host.host = ipv4To;
        }
      }
      if (needUpdate) {
        data.emplace_back(iter->key(), MetaKeyUtils::partVal(partHosts));
      }
      iter->next();
    }
  }

  bool updateSucceed{false};
  folly::Baton<true, std::atomic> baton;
  kvstore_->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        updateSucceed = (code == nebula::cpp2::ErrorCode::SUCCEEDED);
        errMsg_ = folly::stringPrintf("Write to kvstore failed, %s , %d", __func__, __LINE__);
        baton.post();
      });
  baton.wait();
  return updateSucceed;
}

bool MetaHttpReplaceHostHandler::replaceHostInZone(std::string ipv4From, std::string ipv4To) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  const auto& zonePrefix = MetaKeyUtils::zonePrefix();
  std::unique_ptr<kvstore::KVIterator> iter;
  auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, zonePrefix, &iter);
  if (kvRet != nebula::cpp2::ErrorCode::SUCCEEDED) {
    errMsg_ = folly::stringPrintf("Can't get zone prefix=%s", zonePrefix.c_str());
    LOG(INFO) << errMsg_;
    return false;
  }

  std::vector<nebula::kvstore::KV> data;
  while (iter->valid()) {
    bool needUpdate = false;
    auto zoneName = MetaKeyUtils::parseZoneName(iter->key());
    auto hosts = MetaKeyUtils::parseZoneHosts(iter->val());
    for (auto& host : hosts) {
      if (host.host == ipv4From) {
        host.host = ipv4To;
        needUpdate = true;
      }
    }

    if (needUpdate) {
      data.emplace_back(iter->key(), MetaKeyUtils::zoneVal(hosts));
    }
    iter->next();
  }

  bool updateSucceed{false};
  folly::Baton<true, std::atomic> baton;
  kvstore_->asyncMultiPut(
      kDefaultSpaceId, kDefaultPartId, std::move(data), [&](nebula::cpp2::ErrorCode code) {
        updateSucceed = (code == nebula::cpp2::ErrorCode::SUCCEEDED);
        errMsg_ = folly::stringPrintf("Write to kvstore failed, %s , %d", __func__, __LINE__);
        baton.post();
      });
  baton.wait();

  return updateSucceed;
}

}  // namespace meta
}  // namespace nebula
