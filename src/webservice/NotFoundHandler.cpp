/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "webservice/NotFoundHandler.h"
#include <proxygen/httpserver/ResponseBuilder.h>

namespace nebula {

using proxygen::HTTPMessage;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void NotFoundHandler::onRequest(std::unique_ptr<HTTPMessage>) noexcept {
    // Do nothing
}


void NotFoundHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void NotFoundHandler::onEOM() noexcept {
    ResponseBuilder(downstream_)
        .status(404, "Not Found")
        .sendWithEOM();
    return;
}


void NotFoundHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void NotFoundHandler::requestComplete() noexcept {
    delete this;
}


void NotFoundHandler::onError(ProxygenError err) noexcept {
    LOG(ERROR) << "Web service NotFoundHandler got error: "
               << proxygen::getErrorString(err);
    delete this;
}

}  // namespace nebula
