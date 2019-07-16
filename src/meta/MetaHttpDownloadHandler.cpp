/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <stdlib.h>
#include <sys/wait.h>
#include <sys/types.h>
#include "meta/MetaHttpDownloadHandler.h"
#include "meta/MetaServiceUtils.h"
#include "webservice/Common.h"
#include "network/NetworkUtils.h"
#include "hdfs/HdfsHelper.h"
#include "process/ProcessUtils.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

DEFINE_int32(storage_http_port, 12000, "Storage daemon's http port");

namespace nebula {
namespace meta {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void MetaHttpDownloadHandler::init(nebula::kvstore::KVStore *kvstore,
                                   nebula::hdfs::HdfsHelper *helper) {
    kvstore_ = kvstore;
    helper_ = helper;
    CHECK_NOTNULL(kvstore_);
    CHECK_NOTNULL(helper_);
}

void MetaHttpDownloadHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (!headers->hasQueryParam("host") ||
        !headers->hasQueryParam("port") ||
        !headers->hasQueryParam("path") ||
        !headers->hasQueryParam("local") ||
        !headers->hasQueryParam("space")) {
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }

    hdfsHost_ = headers->getQueryParam("host");
    hdfsPort_ = headers->getIntQueryParam("port");
    hdfsPath_ = headers->getQueryParam("path");
    localPath_ = headers->getQueryParam("local");
    spaceID_ = headers->getIntQueryParam("space");
}


void MetaHttpDownloadHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void MetaHttpDownloadHandler::onEOM() noexcept {
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

    if (helper_->checkHadoopPath()) {
        if (dispatchSSTFiles(hdfsHost_, hdfsPort_, hdfsPath_, localPath_)) {
            ResponseBuilder(downstream_)
                .status(200, "SSTFile dispatch successfully")
                .body("SSTFile dispatch successfully")
                .sendWithEOM();
        } else {
            LOG(ERROR) << "SSTFile dispatch failed";
            ResponseBuilder(downstream_)
                .status(404, "SSTFile dispatch failed")
                .body("SSTFile dispatch failed")
                .sendWithEOM();
        }
    } else {
        LOG(ERROR) << "Hadoop Home not exist";
        ResponseBuilder(downstream_)
            .status(404, "HADOOP_HOME not exist")
            .sendWithEOM();
    }
}


void MetaHttpDownloadHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void MetaHttpDownloadHandler::requestComplete() noexcept {
    delete this;
}


void MetaHttpDownloadHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web Service MetaHttpDownloadHandler got error : "
               << proxygen::getErrorString(error);
}

bool MetaHttpDownloadHandler::dispatchSSTFiles(const std::string& hdfsHost,
                                               int hdfsPort,
                                               const std::string& hdfsPath,
                                               const std::string& localPath) {
    auto result = helper_->ls(hdfsHost, hdfsPort, hdfsPath);
    if (!result.ok()) {
        LOG(ERROR) << "Dispatch SSTFile Failed";
        return false;
    }
    std::vector<std::string> files;
    folly::split("\n", result.value(), files, true);
    int32_t  partNumber = files.size() - 1;

    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = MetaServiceUtils::partPrefix(spaceID_);
    auto ret = kvstore_->prefix(0, 0, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Fetch Parts Failed";
        return false;
    }

    int32_t partSize{0};
    std::unordered_map<HostAddr, std::vector<PartitionID>> hostPartition;
    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        for (auto host : MetaServiceUtils::parsePartVal(iter->val())) {
            auto address = std::make_pair(host.get_ip(), host.get_port());
            auto addressIter = hostPartition.find(address);
            if (addressIter == hostPartition.end()) {
                std::vector<PartitionID> partitions;
                hostPartition.emplace(address, partitions);
            }
            hostPartition[address].emplace_back(partId);
        }
        partSize++;
        iter->next();
    }

    if (partNumber != partSize) {
        LOG(ERROR) << "HDFS part number should be equal with nebula "
                   << partNumber << " " << partSize;
        return false;
    }

    std::atomic<int> completed(0);
    std::vector<std::thread> threads;
    for (auto &pair : hostPartition) {
        std::string partsStr;
        folly::join(",", pair.second, partsStr);

        auto storageIP = network::NetworkUtils::intToIPv4(pair.first.first);
        threads.push_back(std::thread([storageIP, hdfsHost, hdfsPort, hdfsPath,
                                       partsStr, localPath, &completed]() {
            auto tmp = "http://%s:%d/download?host=%s&port=%d&path=%s&parts=%s&local=%s";
            auto url = folly::stringPrintf(tmp, storageIP.c_str(), FLAGS_storage_http_port,
                                           hdfsHost.c_str(), hdfsPort, hdfsPath.c_str(),
                                           partsStr.c_str(), localPath.c_str());
            auto command = folly::stringPrintf("/usr/bin/curl -G \"%s\"", url.c_str());
            LOG(INFO) << "Command: " << command;
            auto downloadResult = ProcessUtils::runCommand(command.c_str());
            if (!downloadResult.ok() || downloadResult.value() != "SSTFile download successfully") {
                LOG(ERROR) << "Failed to download SST Files: " << downloadResult.value();
            } else {
                completed++;
            }
        }));
    }

    for (auto &thread : threads) {
        thread.join();
    }
    return completed == (int32_t)hostPartition.size();
}

}  // namespace meta
}  // namespace nebula
