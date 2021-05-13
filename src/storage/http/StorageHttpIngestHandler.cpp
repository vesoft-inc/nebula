/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/http/StorageHttpIngestHandler.h"
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

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

    space_ = headers->getIntQueryParam("space");
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

    if (ingestSSTFiles(space_)) {
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

bool StorageHttpIngestHandler::ingestSSTFiles(GraphSpaceID space) {
    auto code = kvstore_->ingest(space);
    if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
        return true;
    } else {
        LOG(ERROR) << "SSTFile Ingest Failed: "
                   << apache::thrift::util::enumNameSafe(code);
        return false;
    }
}

}  // namespace storage
}  // namespace nebula
