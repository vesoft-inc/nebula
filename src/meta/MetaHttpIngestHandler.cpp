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
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

DEFINE_int32(storage_ingest_http_port, 12000, "Storage daemon's http port");

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
                .status(405, "Method Not Allowed")
                .sendWithEOM();
            return;
        case HttpCode::E_ILLEGAL_ARGUMENT:
            ResponseBuilder(downstream_)
                .status(400, "Bad Request")
                .sendWithEOM();
            return;
        default:
            break;
    }

    if (ingestSSTFiles(space_, path_)) {
        LOG(INFO) << "SSTFile ingest successfully " << path_;
        ResponseBuilder(downstream_)
            .status(200, "SSTFile ingest successfully")
            .body("SSTFile ingest successfully")
            .sendWithEOM();
    } else {
        LOG(ERROR) << "SSTFile ingest failed";
        ResponseBuilder(downstream_)
            .status(404, "SSTFile ingest failed")
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

    std::vector<std::string> storageIPs;
    while (iter->valid()) {
        for (auto host : MetaServiceUtils::parsePartVal(iter->val())) {
            auto storageIP = network::NetworkUtils::intToIPv4(host.get_ip());
            if (std::find(storageIPs.begin(), storageIPs.end(), storageIP) == storageIPs.end()) {
                storageIPs.push_back(std::move(storageIP));
            }
        }
        iter->next();
    }

    std::atomic<int> completed(0);
    std::vector<std::thread> threads;
    for (auto &storageIP : storageIPs) {
        threads.push_back(std::thread([storageIP, space, path, &completed]() {
            auto tmp = "http://%s:%d/ingest?path=%s&space=%d";
            auto url = folly::stringPrintf(tmp, storageIP.c_str(), FLAGS_storage_ingest_http_port,
                                           path.c_str(), space);
            auto command = folly::stringPrintf("/usr/bin/curl -G \"%s\"", url.c_str());
            LOG(INFO) << "Command: " << command;
            auto ingestResult = ProcessUtils::runCommand(command.c_str());
            if (!ingestResult.ok() || ingestResult.value() != "SSTFile ingest successfully") {
                LOG(ERROR) << "Failed to ingest SST Files: " << ingestResult.value();
            } else {
                completed++;
            }
        }));
    }

    for (auto &thread : threads) {
        thread.join();
    }

    return completed == (int32_t)storageIPs.size();
}

}  // namespace meta
}  // namespace nebula
