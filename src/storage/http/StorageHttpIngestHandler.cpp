/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/http/StorageHttpIngestHandler.h"
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

void StorageHttpIngestHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}

void StorageHttpIngestHandler::onEOM() noexcept {
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
        LOG(ERROR) << "SSTFile ingest successfully ";
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

bool StorageHttpIngestHandler::ingestSSTFiles() {
    kvstore::ResultCode code;
    if (edge_.has_value()) {
        LOG(INFO) << folly::stringPrintf(
            "ingest space %d edge %d", spaceID_, edge_.value());
        code = kvstore_->ingestEdge(spaceID_, edge_.value());
    } else if (tag_.has_value()) {
        LOG(INFO) << folly::stringPrintf(
            "ingest space %d tag %d", spaceID_, tag_.value());
        code = kvstore_->ingestTag(spaceID_, tag_.value());
    } else {
        LOG(INFO) << folly::stringPrintf(
            "ingest space %d", spaceID_);
        code = kvstore_->ingest(spaceID_);
    }
    if (code == kvstore::ResultCode::SUCCEEDED) {
        return true;
    } else {
        LOG(ERROR) << "SSTFile Ingest Failed: " << code;
        return false;
    }
}

}  // namespace storage
}  // namespace nebula
