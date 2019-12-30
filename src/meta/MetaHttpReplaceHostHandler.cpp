/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/Common.h"
#include "meta/MetaServiceUtils.h"
#include "meta/MetaHttpReplaceHostHandler.h"
#include "webservice/Common.h"
#include "webservice/WebService.h"
#include "network/NetworkUtils.h"
#include "http/HttpClient.h"
#include "process/ProcessUtils.h"
#include "thread/GenericThreadPool.h"
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

void MetaHttpReplaceHostHandler::init(nebula::kvstore::KVStore *kvstore) {
    kvstore_ = kvstore;
    CHECK_NOTNULL(kvstore_);
}

void MetaHttpReplaceHostHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    LOG(INFO) << __PRETTY_FUNCTION__;

    if (!headers->hasQueryParam("from")) {
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        errMsg_ = "miss argument [from]";
        return;
    }

    if (!headers->hasQueryParam("to")) {
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        errMsg_ = "miss argument [to]";
        return;
    }

    std::string sFrom = headers->getQueryParam("from");
    if (!nebula::network::NetworkUtils::ipv4ToInt(sFrom, ipv4From_)) {
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        errMsg_ = folly::stringPrintf("invalid argument from=%s", sFrom.c_str());
    }

    std::string sTo = headers->getQueryParam("to");
    if (!nebula::network::NetworkUtils::ipv4ToInt(sTo, ipv4To_)) {
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        errMsg_ = folly::stringPrintf("invalid argument to=%s", sTo.c_str());
    }

    LOG(INFO) << folly::stringPrintf("change host info in metadata from %s to %s",
                                     sFrom.c_str(), sTo.c_str());
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
                .status(WebServiceUtils::to(HttpStatusCode::BAD_REQUEST),
                        errMsg_)
                .sendWithEOM();
            return;
        default:
            break;
    }

    if (replaceHost(ipv4From_, ipv4To_)) {
        LOG(INFO) << "Replace Host successfully";
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::OK),
                    WebServiceUtils::toString(HttpStatusCode::OK))
            .body("Replace Host successfully")
            .sendWithEOM();
    } else {
        LOG(ERROR) << "Replace Host failed";
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
    LOG(ERROR) << "Web Service MetaHttpReplaceHostHandler got error : "
               << proxygen::getErrorString(error);
}

bool MetaHttpReplaceHostHandler::replaceHost(IPv4 ipv4From, IPv4 ipv4To) {
    folly::SharedMutex::WriteHolder wHolder(LockUtils::spaceLock());
    const auto& spacePrefix = MetaServiceUtils::spacePrefix();
    std::unique_ptr<kvstore::KVIterator> iter;
    auto kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, spacePrefix, &iter);
    if (kvRet != kvstore::ResultCode::SUCCEEDED) {
        errMsg_ = folly::stringPrintf("can't get space prefix=%s", spacePrefix.c_str());
        LOG(ERROR) << errMsg_;
        return false;
    }

    std::vector<GraphSpaceID> allSpaceId;
    while (iter->valid()) {
        auto spaceId = MetaServiceUtils::spaceId(iter->key());
        allSpaceId.emplace_back(spaceId);
        iter->next();
    }
    LOG(INFO) << "allSpaceId.size()=" << allSpaceId.size();

    std::vector<nebula::kvstore::KV> data;
    for (const auto& spaceId : allSpaceId) {
        const auto& partPrefix = MetaServiceUtils::partPrefix(spaceId);
        kvRet = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, partPrefix, &iter);
        if (kvRet != kvstore::ResultCode::SUCCEEDED) {
            errMsg_ = folly::stringPrintf("can't get partPrefix=%s", partPrefix.c_str());
            LOG(ERROR) << errMsg_;
            return false;
        }

        while (iter->valid()) {
            bool needUpdate = false;
            auto partHosts = MetaServiceUtils::parsePartVal(iter->val());
            for (auto& host : partHosts) {
                if (host.ip == ipv4From) {
                    needUpdate = true;
                    host.ip = ipv4To;
                }
            }
            if (needUpdate) {
                data.emplace_back(iter->key(), MetaServiceUtils::partVal(partHosts));
            }
            iter->next();
        }
    }

    bool updateSucceed{false};
    folly::Baton<true, std::atomic> baton;
    kvstore_->asyncMultiPut(kDefaultSpaceId, kDefaultPartId, std::move(data),
                        [&] (kvstore::ResultCode code) {
                            updateSucceed = (code == kvstore::ResultCode::SUCCEEDED);
                            errMsg_ = folly::stringPrintf("write to kvstore failed, %s , %d",
                                                          __func__, __LINE__);
                            baton.post();
                        });
    baton.wait();
    return updateSucceed;
}

}  // namespace meta
}  // namespace nebula
