/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <stdlib.h>
#include <sys/wait.h>
#include <sys/types.h>
#include "meta/MetaHttpHandler.h"
#include "webservice/Common.h"
#include "network/NetworkUtils.h"
#include "process/ProcessUtils.h"
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

void MetaHttpHandler::init(std::shared_ptr<MetaClient> client) {
    metaClient_ = client;
    CHECK_NOTNULL(metaClient_);
}

void MetaHttpHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (headers->hasQueryParam("returnjson")) {
        returnJson_ = true;
    }

    if (headers->hasQueryParam("method")) {
        method_ = headers->getQueryParam("method");
        if (method_ == "download") {
            if (!headers->hasQueryParam("url") ||
                !headers->hasQueryParam("port") ||
                !headers->hasQueryParam("path") ||
                !headers->hasQueryParam("spaceID")) {
                err_ = HttpCode::E_ILLEGAL_ARGUMENT;
                return;
            }

            hdfsUrl_ = headers->getQueryParam("url");
            hdfsPort_ = headers->getIntQueryParam("port");
            hdfsPath_ = headers->getQueryParam("path");
            localPath_ = headers->getQueryParam("localPath");
            spaceID_ = headers->getIntQueryParam("spaceID");
        }
    }

    auto* statusStr = headers->getQueryParamPtr("daemon");
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
        case HttpCode::E_ILLEGAL_ARGUMENT:
            ResponseBuilder(downstream_)
                .status(400, "Bad Request")
                .sendWithEOM();
            return;
        default:
            break;
    }

    if (method_ == "status") {
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
    } else if (method_ == "download") {
        if (auto hadoopHome = std::getenv("HADOOP_HOME")) {
            LOG(INFO) << "Hadoop Path : " << hadoopHome;
            if (dispatchSSTFiles(hdfsUrl_, hdfsPort_, hdfsPath_, localPath_)) {
                ResponseBuilder(downstream_)
                    .status(200, "SSTFile dispatch successfully")
                    .body("SSTFile dispatch successfully")
                    .sendWithEOM();
            } else {
                LOG(ERROR) << "SSTFile dispatch failed";
                ResponseBuilder(downstream_)
                    .status(404, "SSTFile dispatch failed")
                    .sendWithEOM();
            }

        } else {
            LOG(ERROR) << "Hadoop Home not exist";
            ResponseBuilder(downstream_)
                .status(404, "HADOOP_HOME not exist")
                .sendWithEOM();
        }
    } else {
        LOG(ERROR) << "Bad Request " << method_;
        ResponseBuilder(downstream_)
            .status(400, "Bad Request")
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


std::string MetaHttpHandler::readValue(std::string& statusName) {
    folly::toLowerAscii(statusName);
    if (statusName == "status") {
        return "running";
    } else {
        return "unknown";
    }
}


void MetaHttpHandler::readAllValue(folly::dynamic& vals) {
    for (auto& sn : statusAllNames_) {
        std::string statusValue = readValue(sn);
        addOneStatus(vals, sn, statusValue);
    }
}


folly::dynamic MetaHttpHandler::getStatus() {
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

bool MetaHttpHandler::dispatchSSTFiles(const std::string& url,
                                       int port,
                                       const std::string& path,
                                       const std::string& local) {
    auto command = folly::stringPrintf("hdfs dfs -ls hdfs://%s:%d%s ",
                                       url.c_str(), port, path.c_str());
    auto result = ProcessUtils::runCommand(command.c_str());
    std::vector<std::string> files;
    folly::split("\n", result.value(), files, true);
    auto partNumber = std::count_if(files.begin(), files.end(), [](const std::string& element) {
                                        return element.find("part") != std::string::npos;
                                    });

    auto partResult = metaClient_->getPartsAlloc(spaceID_).get();
    if (!partResult.ok()) {
        return false;
    }

    if ((int64_t)partNumber != metaClient_->partsNum(spaceID_)) {
        LOG(ERROR) << "HDFS part number should be equal with nebula";
        return false;
    }

    auto hosts = metaClient_->listHosts().get();
    if (!hosts.ok()) {
        LOG(ERROR) << "List Hosts Failed: " << hosts.status();
        return false;
    }

    for (auto &address : hosts.value()) {
        auto partMap = metaClient_->getPartsMapFromCache(address);
        auto id = partMap.find(spaceID_);
        if (id == partMap.end()) {
            LOG(ERROR) << "Can't find SpaceID: " << spaceID_;
            return false;
        }
        std::vector<PartitionID> partitions;
        auto partIter = partMap[spaceID_].begin();
        while (partIter != partMap[spaceID_].end()) {
            partitions.emplace_back(partIter->first - 1);
            partIter++;
        }
        std::string parts;
        folly::join(",", partitions, parts);

        auto host = network::NetworkUtils::intToIPv4(address.first);
        auto t = "http://%s:%d/storage?method=download&url=%s&port=%d&path=%s&parts=%s&local=%s";
        auto download = folly::stringPrintf(t, host.c_str(), 22000, url.c_str(), port, path.c_str(),
                                            parts.c_str(), local.c_str());
        command = folly::stringPrintf("curl %s ", download.c_str());
        result = ProcessUtils::runCommand(command.c_str());
        if (!result.ok()) {
            LOG(ERROR) << "Failed to download SST Files: " << result.status();
            return false;
        }
    }

    return true;
}

}  // namespace meta
}  // namespace nebula
