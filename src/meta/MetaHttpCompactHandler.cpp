/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include "meta/MetaHttpCompactHandler.h"
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

void MetaHttpCompactHandler::init(nebula::kvstore::KVStore *kvstore,
                                 nebula::thread::GenericThreadPool *pool) {
    kvstore_ = kvstore;
    pool_ = pool;
    CHECK_NOTNULL(kvstore_);
    CHECK_NOTNULL(pool_);
}

void MetaHttpCompactHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (!headers->hasQueryParam("spaceName")) {
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }

    spaceName_ = headers->getQueryParam("spaceName");
    spaceId_ = headers->getIntQueryParam("spaceId");
}

void MetaHttpCompactHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}

void MetaHttpCompactHandler::onEOM() noexcept {
    switch (err_) {
        case HttpCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                        WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
                .sendWithEOM();
            return;
        case HttpCode::E_ILLEGAL_ARGUMENT:
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::BAD_REQUEST),
                        WebServiceUtils::toString(HttpStatusCode::BAD_REQUEST))
                .sendWithEOM();
            return;
        default:
            break;
    }

    if (compact(spaceName_, spaceId_)) {
        LOG(INFO) << "Compact successfully ";
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::OK),
                    WebServiceUtils::toString(HttpStatusCode::OK))
            .body("Compact successfully")
            .sendWithEOM();
    } else {
        LOG(ERROR) << "Compact failed";
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::FORBIDDEN),
                    WebServiceUtils::toString(HttpStatusCode::FORBIDDEN))
            .body("Compact failed")
            .sendWithEOM();
    }
}

void MetaHttpCompactHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void MetaHttpCompactHandler::requestComplete() noexcept {
    delete this;
}

void MetaHttpCompactHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web Service MetaHttpCompactHandler got error : "
               << proxygen::getErrorString(error);
}

bool MetaHttpCompactHandler::compact(const std::string& spaceName, GraphSpaceID spaceId) {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = MetaServiceUtils::partPrefix(spaceId);

    static const GraphSpaceID metaSpaceId = 0;
    static const PartitionID  metaPartId = 0;
    auto ret = kvstore_->prefix(metaSpaceId, metaPartId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed";
        return false;
    }

    std::set<std::string> storageIPs;
    while (iter->valid()) {
        for (auto &host : MetaServiceUtils::parsePartVal(iter->val())) {
            auto storageIP = network::NetworkUtils::intToIPv4(host.get_ip());
            if (std::find(storageIPs.begin(), storageIPs.end(), storageIP) == storageIPs.end()) {
                storageIPs.insert(std::move(storageIP));
            }
        }
        iter->next();
    }

    std::vector<folly::SemiFuture<bool>> futures;
    for (auto &storageIP : storageIPs) {
        auto dispatcher = [storageIP, spaceName]() {
            static const char *tmp = "http://%s:%d/admin?space=%s&op=compact";
            auto url = folly::stringPrintf(tmp, storageIP.c_str(),
                                           FLAGS_ws_storage_http_port,
                                           spaceName.c_str());
            auto compactResult = nebula::http::HttpClient::get(url);
            return compactResult.ok() && compactResult.value() == "Compact successfully";
        };
        auto future = pool_->addTask(dispatcher);
        futures.push_back(std::move(future));
    }

    bool successfully{true};
    folly::collectAll(std::move(futures)).then([&](const std::vector<folly::Try<bool>>& tries) {
        for (const auto& t : tries) {
            if (t.hasException()) {
                LOG(ERROR) << "Compact Failed: " << t.exception();
                successfully = false;
                break;
            }
            if (!t.value()) {
                successfully = false;
                break;
            }
        }
    }).wait();
    LOG(INFO) << "Compact tasks have finished";
    return successfully;
}

}  // namespace meta
}  // namespace nebula
