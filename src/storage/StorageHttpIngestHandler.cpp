/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageHttpIngestHandler.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

namespace nebula {
namespace storage {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void StorageHttpIngestHandler::init(nebula::kvstore::KVStore *kvstore) {
    kvstore_ = kvstore;
    CHECK_NOTNULL(kvstore_);
}

void StorageHttpIngestHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (!headers->hasQueryParam("path")) {
        LOG(ERROR) << "Illegal Argument";
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }
    path_ = headers->getQueryParam("path");
}

void StorageHttpIngestHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}

void StorageHttpIngestHandler::onEOM() noexcept {
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

    if (ingestSSTFiles(path_)) {
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

void StorageHttpIngestHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void StorageHttpIngestHandler::requestComplete() noexcept {
    delete this;
}

void StorageHttpIngestHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web Service MetaHttpIngestHandler Failed: "
               << proxygen::getErrorString(error);
}

bool StorageHttpIngestHandler::ingestSSTFiles(const std::string& path) {
    UNUSED(path);
    return true;
}

}  // namespace storage
}  // namespace nebula
