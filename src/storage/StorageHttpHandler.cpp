/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageHttpHandler.h"
#include "webservice/Common.h"
#include "process/ProcessUtils.h"
#include "fs/FileUtils.h"
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

void StorageHttpHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (headers->hasQueryParam("returnjson")) {
        returnJson_ = true;
    }

    if (headers->hasQueryParam("method")) {
        method = headers->getQueryParam("method");
        if (method == "download") {
            if (!headers->hasQueryParam("url") ||
                !headers->hasQueryParam("path")) {
                err_ = HttpCode::E_ILLEGAL_ARGUMENT;
                return;
            }
            hdfsUrl = headers->getQueryParam("url");
            localPath = headers->getQueryParam("path");
        }
    }

    auto* statusStr = headers->getQueryParamPtr("daemon");
    if (statusStr != nullptr) {
        folly::split(",", *statusStr, statusNames_, true);
    }
}


void StorageHttpHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void StorageHttpHandler::onEOM() noexcept {
    switch (err_) {
        case HttpCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(405, "Method Not Allowed")
                .sendWithEOM();
            return;
        case HttpCode::E_ILLEGAL_ARGUMENT:
            ResponseBuilder(downstream_)
                .status(400, "Illegal Argument")
                .sendWithEOM();
        default:
            break;
    }

    if (method == "status") {
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
    } else if (method == "download") {
        if (auto hadoopHome = std::getenv("HADOOP_HOME")) {
            LOG(INFO) << "Hadoop Path : " << hadoopHome;
            std::vector<std::string> urls;
            if (downloadSSTFiles(urls, localPath)) {
                ResponseBuilder(downstream_)
                    .status(200, "SSTFile download successfully")
                    .body("SSTFile download successfully")
                    .sendWithEOM();
            } else {
                ResponseBuilder(downstream_)
                    .status(404, "SSTFile download failed")
                    .body("SSTFile download failed")
                    .sendWithEOM();
            }
        } else {
            ResponseBuilder(downstream_)
                .status(404, "HADOOP_HOME not exist")
                .body("HADOOP_HOME not exist")
                .sendWithEOM();
        }
    } else {
        LOG(ERROR) << "";
    }
}


void StorageHttpHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void StorageHttpHandler::requestComplete() noexcept {
    delete this;
}


void StorageHttpHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web service StorageHttpHandler got error: "
               << proxygen::getErrorString(error);
}


void StorageHttpHandler::addOneStatus(folly::dynamic& vals,
                                      const std::string& statusName,
                                      const std::string& statusValue) const {
    folly::dynamic status = folly::dynamic::object();
    status["name"] = statusName;
    status["value"] = statusValue;
    vals.push_back(std::move(status));
}


std::string StorageHttpHandler::readValue(std::string& statusName) {
    folly::toLowerAscii(statusName);
    if (statusName == "status") {
        return "running";
    } else {
        return "unknown";
    }
}


void StorageHttpHandler::readAllValue(folly::dynamic& vals) {
    for (auto& sn : statusAllNames_) {
        std::string statusValue = readValue(sn);
        addOneStatus(vals, sn, statusValue);
    }
}


folly::dynamic StorageHttpHandler::getStatus() {
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


std::string StorageHttpHandler::toStr(folly::dynamic& vals) const {
    std::stringstream ss;
    for (auto& counter : vals) {
        auto& val = counter["value"];
        ss << counter["name"].asString() << "="
           << val.asString()
           << "\n";
    }
    return ss.str();
}

bool StorageHttpHandler::downloadSSTFiles(const std::vector<std::string>& urls,
                                          const std::string& path) {
    if (fs::FileUtils::fileType(path.c_str()) == fs::FileType::NOTEXIST) {
        if (fs::FileUtils::makeDir(path)) {
            LOG(INFO) << "Create directory successfully " << path;
        } else {
            LOG(ERROR) << "Create directory failed " << path;
        }
    }

    for (auto url : urls) {
        auto command = folly::stringPrintf("bin/hdfs dfs -copyToLocal %s %s",
                                           url.c_str(), path.c_str());
        LOG(INFO) << "Download SST Files " << command;
        auto result = ProcessUtils::runCommand(command.c_str());
        if (!result.ok()) {
            LOG(ERROR) << "Failed to download SST Files: " << result.status();
            return false;
        }
    }
    return true;
}

bool StorageHttpHandler::ingestSSTFiles(const std::string& path,
                                        GraphSpaceID spaceID) {
    UNUSED(path); UNUSED(spaceID);
    return false;
}

}  // namespace storage
}  // namespace nebula
