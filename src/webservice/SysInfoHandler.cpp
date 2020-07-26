/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "fs/FileUtils.h"
#include "process/ProcessUtils.h"
#include "webservice/SysInfoHandler.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

namespace nebula {

using nebula::ProcessUtils;
using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void SysInfoHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }
}

void SysInfoHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}

void SysInfoHandler::onEOM() noexcept {
    switch (err_) {
        case HttpCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                        WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
                .sendWithEOM();
            return;
        default:
            break;
    }

    folly::dynamic vals = getSysInfo();
    ResponseBuilder(downstream_)
        .status(WebServiceUtils::to(HttpStatusCode::OK),
                WebServiceUtils::toString(HttpStatusCode::OK))
        .body(folly::toJson(vals))
        .sendWithEOM();
}


void SysInfoHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void SysInfoHandler::requestComplete() noexcept {
    delete this;
}

void SysInfoHandler::init(std::vector<std::string> data_path) {
    dataPaths_ = data_path;
}

void SysInfoHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web service StorageHttpHandler got error: "
               << proxygen::getErrorString(error);
}

folly::dynamic SysInfoHandler::getSysInfo() {
    folly::dynamic json = folly::dynamic::object();
    for (long unsigned int i = 0; i < dataPaths_.size(); i++) {
        auto res = fs::FileUtils::detectFilesystemUsage(dataPaths_[i]);
        json["data_path_" + dataPaths_[i]] = folly::dynamic::object();
        json["data_path_" + dataPaths_[i]]["disk_total"] = std::get<0>(res);
        json["data_path_" + dataPaths_[i]]["disk_avail"] = std::get<1>(res);
    }

    auto memUsage = ProcessUtils::getMemUsage();
    json["memory_usage"] = memUsage;

    auto cpuUsage = ProcessUtils::getCpuUsage();
    json["cpu_usage"] = cpuUsage;
    return json;
}

}  // namespace nebula
