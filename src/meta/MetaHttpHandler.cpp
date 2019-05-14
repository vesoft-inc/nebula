/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "webservice/Common.h"
#include "meta/MetaHttpHandler.h"
#include <proxygen/httpserver/RequestHandler.h>

#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

namespace nebula {
namespace meta {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;

void MetaHttpHandler::onRequest(std::unique_ptr<HTTPMessage>) noexcept {
}

void MetaHttpHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
}

void MetaHttpHandler::onEOM() noexcept {
}

void MetaHttpHandler::onUpgrade(UpgradeProtocol) noexcept {
}

void MetaHttpHandler::requestComplete() noexcept {
    delete this;
}

void MetaHttpHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Meta Handler : "
               << proxygen::getErrorString(error);
}

}  // namespace meta
}  // namespace nebula
