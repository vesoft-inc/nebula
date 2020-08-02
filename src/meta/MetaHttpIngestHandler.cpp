/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include "meta/MetaHttpIngestHandler.h"
#include "webservice/Common.h"
#include "webservice/WebService.h"
#include "network/NetworkUtils.h"
#include "http/HttpClient.h"
#include "process/ProcessUtils.h"
#include "thread/GenericThreadPool.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

DEFINE_int32(meta_ingest_thread_num, 3, "Meta daemon's ingest thread number");

namespace nebula {
namespace meta {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void MetaHttpIngestHandler::init(nebula::kvstore::KVStore *kvstore,
                                 nebula::thread::GenericThreadPool *pool) {
    kvstore_ = kvstore;
    pool_ = pool;
    CHECK_NOTNULL(kvstore_);
    CHECK_NOTNULL(pool_);
}

void MetaHttpIngestHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (!headers->hasQueryParam("space")) {
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }
    spaceID_ = headers->getIntQueryParam("space");

    try {
        if (headers->hasQueryParam("tag")) {
            auto& tag = headers->getQueryParam("tag");
            tag_.assign(folly::to<TagID>(tag));
        }

        if (headers->hasQueryParam("edge")) {
            auto& edge = headers->getQueryParam("edge");
            edge_.assign(folly::to<EdgeType>(edge));
        }
    } catch (std::exception& e) {
        LOG(ERROR) << "Parse tag/edge error. " << e.what();
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }
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

    if (ingestSSTFiles()) {
        LOG(INFO) << "SSTFile ingest successfully ";
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

bool MetaHttpIngestHandler::ingestSSTFiles() {
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = MetaServiceUtils::partPrefix(spaceID_);
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
        std::string url;
        if (edge_.has_value()) {
            url = folly::stringPrintf(
                "http://%s:%d/ingest?space=%d&edge=%d",
                storageIP.c_str(), FLAGS_ws_storage_http_port,
                spaceID_, edge_.value());
        } else if (tag_.has_value()) {
            url = folly::stringPrintf(
                "http://%s:%d/ingest?space=%d&tag=%d",
                storageIP.c_str(), FLAGS_ws_storage_http_port,
                spaceID_, tag_.value());
        } else {
            url = folly::stringPrintf(
                "http://%s:%d/ingest?space=%d",
                storageIP.c_str(), FLAGS_ws_storage_http_port,
                spaceID_);
        }
        auto dispatcher = [url] {
            auto ingestResult = nebula::http::HttpClient::get(url);
            if (ingestResult.ok() && ingestResult.value() == "SSTFile ingest successfully") {
                return true;
            }
            LOG(ERROR) << "Ingest Failed, url: " << url << " error: " << ingestResult.value();
            return false;
        };
        auto future = pool_->addTask(dispatcher);
        futures.push_back(std::move(future));
    }

    bool successfully{true};
    folly::collectAll(std::move(futures)).thenValue(
        [&](const std::vector<folly::Try<bool>>& tries) {
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
