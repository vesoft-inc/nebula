/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "graph/GraphHttpHandler.h"

#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

namespace nebula {
namespace graph {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;

void GraphHttpHandler::onRequest(std::unique_ptr<HTTPMessage>) noexcept {
}

void GraphHttpHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
}

void GraphHttpHandler::onEOM() noexcept {
}

void GraphHttpHandler::onUpgrade(UpgradeProtocol) noexcept {
}

void GraphHttpHandler::requestComplete() noexcept {
    delete this;
}

void GraphHttpHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Graph Handler : "
               << proxygen::getErrorString(error);
}

}  // namespace graph
}  // namespace nebula
