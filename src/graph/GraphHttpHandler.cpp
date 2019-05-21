/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/GraphHttpHandler.h"
#include "webservice/Common.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

namespace nebula {
namespace graph {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void GraphHttpHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (headers->getQueryParamPtr("returnjson") != nullptr) {
        returnJson_ = true;
    }

    auto* statusStr = headers->getQueryParamPtr("daemon");
    if (statusStr != nullptr) {
        folly::split(",", *statusStr, statusNames_, true);
    }
}


void GraphHttpHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void GraphHttpHandler::onEOM() noexcept {
    switch (err_) {
        case HttpCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(405, "Method Not Allowed")
                .sendWithEOM();
            return;
        default:
            break;
    }

    // read graph daemon status
    folly::dynamic vals = getStatus();
    if (returnJson_) {
        ResponseBuilder(downstream_)
            .status(200, "OK")
            .body(folly::toJson(vals))
            .sendWithEOM();
    } else {
        ResponseBuilder(downstream_)
            .status(200, "OK")
            .body(toStr(vals))
            .sendWithEOM();
    }
}


void GraphHttpHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void GraphHttpHandler::requestComplete() noexcept {
    delete this;
}


void GraphHttpHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web service GraphHttpHandler got error : "
               << proxygen::getErrorString(error);
}


void GraphHttpHandler::addOneStatus(folly::dynamic& vals,
                                    const std::string& statusName,
                                    const std::string& statusValue) const {
    folly::dynamic status = folly::dynamic::object();
    status["name"] = statusName;
    status["value"] = statusValue;
    vals.push_back(std::move(status));
}


std::string GraphHttpHandler::readValue(std::string& statusName) {
    folly::toLowerAscii(statusName);
    if (statusName == "status") {
        return "running";
    } else {
        return "unknown";
    }
}


void GraphHttpHandler::readAllValue(folly::dynamic& vals) {
    for (auto& sn : statusAllNames_) {
        std::string statusValue = readValue(sn);
        addOneStatus(vals, sn, statusValue);
    }
}


folly::dynamic GraphHttpHandler::getStatus() {
    auto status = folly::dynamic::array();
    if (statusNames_.empty()) {
        // Read all status
        readAllValue(status);
    } else {
        for (auto& sn : statusNames_) {
            std::string statusValue = readValue(sn);
            addOneStatus(status, sn, statusValue);
        }
    }
    return status;
}


std::string GraphHttpHandler::toStr(folly::dynamic& vals) const {
    std::stringstream ss;
    for (auto& counter : vals) {
        auto& val = counter["value"];
        ss << counter["name"].asString() << "="
           << val.asString()
           << "\n";
    }
    return ss.str();
}

}  // namespace graph
}  // namespace nebula
