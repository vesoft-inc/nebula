/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageHttpAdminHandler.h"
#include "webservice/Common.h"
#include "process/ProcessUtils.h"
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

void StorageHttpAdminHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }
    if (schemaMan_ == nullptr || kv_ == nullptr) {
        err_ = HttpCode::SUCCEEDED;
        resp_ = folly::stringPrintf("Error inside");
        return;
    }
    auto* space = headers->getQueryParamPtr("space");
    if (space == nullptr) {
        err_ = HttpCode::SUCCEEDED;
        resp_ = "Space should not be empty. Usage: http:://ip:port/admin?space=xx&op=yy";
        return;
    }
    auto* op = headers->getQueryParamPtr("op");
    if (op == nullptr) {
        err_ = HttpCode::SUCCEEDED;
        resp_ = "Op should not be empty. Usage: http:://ip:port/admin?space=xx&op=yy";
        return;
    }
    auto ret = schemaMan_->toGraphSpaceID(*space);
    if (!ret.ok()) {
        err_ = HttpCode::SUCCEEDED;
        resp_ = folly::stringPrintf("Can't find space %s", space->c_str());
        return;
    }
    auto spaceId = ret.value();

    if (*op == "compact") {
        LOG(INFO) << "do compact at space=" << *space;
        auto status = kv_->compact(spaceId);
        if (status != kvstore::ResultCode::SUCCEEDED) {
            resp_ = folly::stringPrintf("Compact failed! error=%d", static_cast<int32_t>(status));
            err_ = HttpCode::SUCCEEDED;
            return;
        }
    } else if (*op == "flush") {
        auto status = kv_->flush(spaceId);
        if (status != kvstore::ResultCode::SUCCEEDED) {
            resp_ = folly::stringPrintf("Flush failed! error=%d", static_cast<int32_t>(status));
            err_ = HttpCode::SUCCEEDED;
            return;
        }
    } else {
        resp_ = folly::stringPrintf("Unknown operation %s", op->c_str());
        err_ = HttpCode::SUCCEEDED;
        return;
    }
    resp_ = folly::stringPrintf("ok");
    err_ = HttpCode::SUCCEEDED;
    return;
}


void StorageHttpAdminHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void StorageHttpAdminHandler::onEOM() noexcept {
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

    ResponseBuilder(downstream_)
        .status(200, "OK")
        .body(resp_)
        .sendWithEOM();
}


void StorageHttpAdminHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void StorageHttpAdminHandler::requestComplete() noexcept {
    delete this;
}


void StorageHttpAdminHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web service StorageHttpHandler got error: "
               << proxygen::getErrorString(error);
}


}  // namespace storage
}  // namespace nebula
