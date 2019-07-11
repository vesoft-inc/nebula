/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaHttpIngestHandler.h"
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

    if (!headers->hasQueryParam("path")) {
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }

    path_ = headers->getQueryParam("path");
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

bool MetaHttpIngestHandler::ingestSSTFiles(const std::string& path) {
    UNUSED(path);
    return true;
}

}  // namespace meta
}  // namespace nebula
