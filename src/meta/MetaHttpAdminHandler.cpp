/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include "meta/MetaHttpAdminHandler.h"
#include "webservice/Common.h"
#include "webservice/WebService.h"
#include "network/NetworkUtils.h"
#include "http/HttpClient.h"
#include "process/ProcessUtils.h"
#include "thread/GenericThreadPool.h"
#include "meta/KVJobManager.h"
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

void MetaHttpAdminHandler::init(nebula::kvstore::KVStore *kvstore,
                                 nebula::thread::GenericThreadPool *pool) {
    LOG(INFO) << "MetaHttpAdminHandler::init";
    kvstore_ = kvstore;
    pool_ = pool;
    CHECK_NOTNULL(kvstore_);
    CHECK_NOTNULL(pool_);
}

void MetaHttpAdminHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (!headers->hasQueryParam("op") || !headers->hasQueryParam("spaceName")) {
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }

    op_ = headers->getQueryParam("op");
    for (char& c : op_) {
        c = std::tolower(c);
    }
    spaceName_ = headers->getQueryParam("spaceName");

    if (headers->hasQueryParam("spaceId")) {
        spaceId_ = headers->getIntQueryParam("spaceId");
    }

    int i = 0;
    for (;;) {
        std::string key = "para" + std::to_string(i++);
        LOG(WARNING) << " check key " << key;
        if (!headers->hasQueryParam(key)) {
            break;
        }
        paras_.emplace_back(headers->getQueryParam(key));
    }
    paras_.emplace_back(spaceName_);
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
                .sendWithEOM();
            LOG(INFO) << "error E_UNSUPPORTED_METHOD";
            return;
        case HttpCode::E_ILLEGAL_ARGUMENT:
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::BAD_REQUEST),
                        WebServiceUtils::toString(HttpStatusCode::BAD_REQUEST))
                .sendWithEOM();
            LOG(INFO) << "error E_ILLEGAL_ARGUMENT";
            return;
        default:
            break;
    }

    auto job_mgr = KVJobManager::getInstance();
    auto resp = job_mgr->runJob(op_, paras_);
    if (ok(resp)) {
        LOG(INFO) << "admin successfully ";
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::OK),
                    WebServiceUtils::toString(HttpStatusCode::OK))
            .body(value(resp))
            .sendWithEOM();
    } else {
        LOG(ERROR) << "admin failed";
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::FORBIDDEN),
                    WebServiceUtils::toString(HttpStatusCode::FORBIDDEN))
            .body(error(resp))
            .sendWithEOM();
    }
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

bool MetaHttpAdminHandler::RunJob(const std::string& op,
    const std::string& spaceName, GraphSpaceID spaceId) {
    return syncRunJob(op, spaceName, spaceId);
}

// std::pair<bool, std::string>
ErrorOr<nebula::kvstore::ResultCode, std::string>
MetaHttpAdminHandler::asyncRunJob(const std::string& op,
                                                               const std::string& para) {
    LOG(WARNING) << __func__ << " op=" << op << ", name=" << para;
    auto job_mgr = KVJobManager::getInstance();
    return job_mgr->runJob(op, paras_);
}

bool MetaHttpAdminHandler::syncRunJob(const std::string& op,
    const std::string& spaceName, GraphSpaceID spaceId) {
    LOG(WARNING) << "admin run job = " << op;
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
        auto dispatcher = [op, storageIP, spaceName]() {
            static const char *tmp = "http://%s:%d/admin?space=%s&op=%s";
            auto url = folly::stringPrintf(tmp, storageIP.c_str(),
                                           FLAGS_ws_storage_http_port,
                                           spaceName.c_str(), op.c_str());
            LOG(INFO) << "make admin url: " << url;
            auto AdminResult = nebula::http::HttpClient::get(url);
            return AdminResult.ok() && AdminResult.value() == "admin successfully";
        };
        auto future = pool_->addTask(dispatcher);
        futures.push_back(std::move(future));
    }

    bool successfully{true};
    folly::collectAll(std::move(futures))
        .thenValue([&](const std::vector<folly::Try<bool>>& tries) {
        for (const auto& t : tries) {
            if (t.hasException()) {
                LOG(ERROR) << "admin Failed: " << t.exception();
                successfully = false;
                break;
            }
            if (!t.value()) {
                successfully = false;
                break;
            }
        }
    }).wait();
    LOG(INFO) << "admin tasks have finished";
    return successfully;
}

}  // namespace meta
}  // namespace nebula
