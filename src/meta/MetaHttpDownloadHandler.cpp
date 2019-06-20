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

void MetaHttpDownloadHandler::init(nebula::kvstore::KVStore *kvstore) {
    kvstore_ = kvstore;
    CHECK_NOTNULL(kvstore_);
}

void MetaHttpDownloadHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

    if (headers->hasQueryParam("returnjson")) {
        returnJson_ = true;
    }

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
}


void MetaHttpDownloadHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void MetaHttpDownloadHandler::requestComplete() noexcept {
    delete this;
}


void MetaHttpDownloadHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web service MetaHttpDownloadHandler got error : "
               << proxygen::getErrorString(error);
}

std::string MetaHttpDownloadHandler::toStr(folly::dynamic& vals) const {
    std::stringstream ss;
    for (auto& counter : vals) {
        auto& val = counter["value"];
        ss << counter["name"].asString() << "="
           << val.asString()
           << "\n";
    }
    return ss.str();
}

bool MetaHttpDownloadHandler::dispatchSSTFiles(const std::string& url,
                                       int port,
                                       const std::string& path,
                                       const std::string& local) {
    auto command = folly::stringPrintf("hdfs dfs -ls hdfs://%s:%d%s ",
                                       url.c_str(), port, path.c_str());
    /**
     * HDFS command output looks like:
     * -rw-r--r--   1 user supergroup 0 2019-06-11 10:27 hdfs://host:port/path/part-{part_number}
     **/
    auto result = ProcessUtils::runCommand(command.c_str());
    std::vector<std::string> files;
    folly::split("\n", result.value(), files, true);
    auto partNumber = std::count_if(files.begin(), files.end(), [](const std::string& element) {
                                        return element.find("part") != std::string::npos;
                                    });

    std::vector<cpp2::HostItem> hostItems;
    std::unordered_map<HostAddr, std::vector<PartitionID>> hostPartition;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto prefix = MetaServiceUtils::partPrefix(spaceID_);
    auto ret = kvstore_->prefix(0, 0, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return false;
    }
    while (iter->valid()) {
        cpp2::HostItem item;
        nebula::cpp2::HostAddr host;
        auto hostAddrPiece = iter->key().subpiece(prefix.size());
        memcpy(&host, hostAddrPiece.data(), hostAddrPiece.size());
        item.set_hostAddr(host);
        auto address = std::make_pair(host.get_ip(), host.get_port());
        auto hostIter = hostPartition.find(address);
        if (hostIter == hostPartition.end()) {
            std::vector<PartitionID> partitions;
            hostPartition.insert(std::make_pair(address, partitions));
        }
        hostItems.emplace_back(item);
        iter->next();
    }

    int32_t partSize;
    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        for (auto host : MetaServiceUtils::parsePartVal(iter->val())) {
            auto address = std::make_pair(host.get_ip(), host.get_port());
            hostPartition[address].emplace_back(partId);
        }
        partSize++;
        iter->next();
    }

    if (partNumber != partSize) {
        LOG(ERROR) << "HDFS part number should be equal with nebula";
        return false;
    }

    std::atomic<int> completed(0);
    std::vector<std::thread> threads;
    for (auto &pair : hostPartition) {
        std::vector<PartitionID> partitions;
        for (auto part : pair.second) {
            partitions.emplace_back(part - 1);
        }
        std::string partNumbers;
        folly::join(",", partitions, partNumbers);

        auto host = network::NetworkUtils::intToIPv4(pair.first.first);
        auto t = "http://%s:%d/storage?method=download&url=%s&port=%d&path=%s&parts=%s&local=%s";
        auto download = folly::stringPrintf(t, host.c_str(), 22000, url.c_str(), port, path.c_str(),
                                            partNumbers.c_str(), local.c_str());
        command = folly::stringPrintf("curl %s ", download.c_str());
        threads.push_back(std::thread([&]() {
            auto downloadResult = ProcessUtils::runCommand(command.c_str());
            if (!downloadResult.ok()) {
                LOG(ERROR) << "Failed to download SST Files: " << downloadResult.status();
            } else {
                completed++;
            }
        }));
    }

    for (auto &thread : threads) {
        thread.join();
    }
    if (completed == (int32_t)hostPartition.size()) {
        return true;
    } else {
        return false;
    }
}

}  // namespace meta
}  // namespace nebula
