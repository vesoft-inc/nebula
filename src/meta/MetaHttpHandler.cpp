/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/MetaHttpHandler.h"
#include "webservice/Common.h"
#include "base/StringSwitch.h"
#include "process/ProcessUtils.h"
#include "meta/MetaFlags.h"
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
using nebula::ProcessUtils;

void MetaHttpHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (headers->getQueryParamPtr("returnjson") != nullptr) {
        returnJson_ = true;
    }

    auto* statusStr = headers->getQueryParamPtr("meta");
    if (statusStr != nullptr) {
        folly::split(",", *statusStr, statusNames_, true);
    }
}


void MetaHttpHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void MetaHttpHandler::onEOM() noexcept {
    switch (err_) {
        case HttpCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(405, "Method Not Allowed")
                .sendWithEOM();
            return;
        default:
            break;
    }

    // read meta daemon status
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


void MetaHttpHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void MetaHttpHandler::requestComplete() noexcept {
    delete this;
}


void MetaHttpHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web service MetaHttpHandler got error : "
               << proxygen::getErrorString(error);
}


void MetaHttpHandler::addOneStatus(folly::dynamic& vals,
                                   const std::string& statusName,
                                   const std::string& statusValue) const {
    folly::dynamic status = folly::dynamic::object();
    status["name"] = statusName;
    status["value"] = statusValue;
    vals.push_back(std::move(status));
}


std::string MetaHttpHandler::readValue(const std::string& statusName) const {
    Status ret;
    switch (StringSwitch(statusName.c_str())) {
    case "status"_hash:
        ret = ProcessUtils::isPidRunning(FLAGS_meta_pid_file);
        if (ret.ok()) {
            return "running";
        } else {
            return "stop";
        }
    default:
        return "unknown";
    }
}


void MetaHttpHandler::readAllValue(folly::dynamic& vals) const {
    for (auto& sn : statusAllNames_) {
        std::string statusValue = readValue(sn);
        addOneStatus(vals, sn, statusValue);
    }
}


folly::dynamic MetaHttpHandler::getStatus() const {
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


std::string MetaHttpHandler::toStr(folly::dynamic& vals) const {
    std::stringstream ss;
    for (auto& counter : vals) {
        auto& val = counter["value"];
        ss << counter["name"].asString() << "="
           << val.asString()
           << "\n";
    }
    return ss.str();
}

}  // namespace meta
}  // namespace nebula
