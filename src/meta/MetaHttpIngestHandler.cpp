/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include "meta/MetaHttpIngestHandler.h"
#include "webservice/Common.h"
#include "network/NetworkUtils.h"
#include "process/ProcessUtils.h"
#include "thread/GenericThreadPool.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

DEFINE_int32(storage_ingest_http_port, 12000, "Storage daemon's http port");
DEFINE_int32(meta_ingest_thread_num, 3, "Meta daemon's ingest thread number");

namespace nebula {
namespace meta {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void MetaHttpIngestHandler::init(nebula::kvstore::KVStore *kvstore) {
    kvstore_ = kvstore;
    CHECK_NOTNULL(kvstore_);
}

void MetaHttpIngestHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (!headers->hasQueryParam("path") ||
        !headers->hasQueryParam("space")) {
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }

    path_ = headers->getQueryParam("path");
    space_ = headers->getIntQueryParam("space");
}

void MetaHttpIngestHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void MetaHttpIngestHandler::onEOM() noexcept {
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

    if (ingestSSTFiles(space_, path_)) {
        LOG(INFO) << "SSTFile ingest successfully " << path_;
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::OK),
                    WebServiceUtils::toString(HttpStatusCode::OK))
            .body("SSTFile ingest successfully")
            .sendWithEOM();
    } else {
        LOG(ERROR) << "SSTFile ingest failed";
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::FORBIDDEN),
                    WebServiceUtils::toString(HttpStatusCode::FORBIDDEN))
            .body("SSTFile ingest failed")
            .sendWithEOM();
    }
}

void MetaHttpIngestHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void MetaHttpIngestHandler::requestComplete() noexcept {
    delete this;
}


void MetaHttpIngestHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web Service MetaHttpIngestHandler got error : "
               << proxygen::getErrorString(error);
}

bool MetaHttpIngestHandler::ingestSSTFiles(GraphSpaceID space, const std::string& path) {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = MetaServiceUtils::partPrefix(space);
    auto ret = kvstore_->prefix(0, 0, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed";
        return false;
    }

    std::set<std::string> storageIPs;
    while (iter->valid()) {
        for (auto host : MetaServiceUtils::parsePartVal(iter->val())) {
            auto storageIP = network::NetworkUtils::intToIPv4(host.get_ip());
            if (std::find(storageIPs.begin(), storageIPs.end(), storageIP) == storageIPs.end()) {
                storageIPs.insert(std::move(storageIP));
            }
        }
        iter->next();
    }

    std::vector<folly::SemiFuture<bool>> futures;
    static nebula::thread::GenericThreadPool pool;
    static std::once_flag ingestPoolStartFlag;
    std::call_once(ingestPoolStartFlag, []() {
        LOG(INFO) << "Ingest Thread Pool start";
        pool.start(FLAGS_meta_ingest_thread_num);
    });

    for (auto &storageIP : storageIPs) {
        auto dispatcher = [storageIP, space, path]() {
            auto tmp = "http://%s:%d/ingest?path=%s&space=%d";
            auto url = folly::stringPrintf(tmp, storageIP.c_str(), FLAGS_storage_ingest_http_port,
                                           path.c_str(), space);
            auto command = folly::stringPrintf("/usr/bin/curl -G \"%s\"", url.c_str());
            LOG(INFO) << "Command: " << command;
            auto ingestResult = ProcessUtils::runCommand(command.c_str());
            return ingestResult.ok() && ingestResult.value() == "SSTFile ingest successfully";
        };
        auto future = pool.addTask(dispatcher);
        futures.push_back(std::move(future));
    }

    bool successfully{true};
    folly::collectAll(futures).then([&](const std::vector<folly::Try<bool>>& tries) {
        for (const auto& t : tries) {
            if (t.hasException()) {
                LOG(ERROR) << "Ingest Failed: " << t.exception();
                successfully = false;
                break;
            }
            if (!t.value()) {
                successfully = false;
                break;
            }
        }
    }).wait();
    LOG(INFO) << "Ingest tasks have finished";
    return successfully;
}

}  // namespace meta
}  // namespace nebula
