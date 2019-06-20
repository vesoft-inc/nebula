/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageHttpDownloadHandler.h"
#include "webservice/Common.h"
#include "process/ProcessUtils.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>

DEFINE_int32(download_thread_num, 3, "download thread number");

namespace nebula {
namespace storage {

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void StorageHttpDownloadHandler::init(nebula::kvstore::KVStore *kvstore) {
    kvstore_ = kvstore;
    CHECK_NOTNULL(kvstore_);
}

void StorageHttpDownloadHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
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
         !headers->hasQueryParam("parts") ||
         !headers->hasQueryParam("local")) {
         err_ = HttpCode::E_ILLEGAL_ARGUMENT;
         return;
     }
     hdfsUrl_ = headers->getQueryParam("url");
     hdfsPort_ = headers->getIntQueryParam("port");
     partitions_ = headers->getQueryParam("parts");
     hdfsPath_ = headers->getQueryParam("path");
     localPath_ = headers->getQueryParam("local");
}


void StorageHttpDownloadHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void StorageHttpDownloadHandler::onEOM() noexcept {
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
        std::vector<std::string> parts;
        folly::split(",", partitions_, parts, true);
        if (downloadSSTFiles(hdfsUrl_, hdfsPort_, hdfsPath_, parts, localPath_)) {
            ResponseBuilder(downstream_)
                .status(200, "SSTFile download successfully")
                .body("SSTFile download successfully")
                .sendWithEOM();
        } else {
            ResponseBuilder(downstream_)
                .status(404, "SSTFile download failed")
                .sendWithEOM();
        }
    } else {
        ResponseBuilder(downstream_)
            .status(404, "HADOOP_HOME not exist")
            .sendWithEOM();
    }
}


void StorageHttpDownloadHandler::onUpgrade(UpgradeProtocol) noexcept {
    // Do nothing
}


void StorageHttpDownloadHandler::requestComplete() noexcept {
    delete this;
}


void StorageHttpDownloadHandler::onError(ProxygenError error) noexcept {
    LOG(ERROR) << "Web service StorageHttpDownloadHandler got error: "
               << proxygen::getErrorString(error);
}

std::string StorageHttpDownloadHandler::toStr(folly::dynamic& vals) const {
    std::stringstream ss;
    for (auto& counter : vals) {
        auto& val = counter["value"];
        ss << counter["name"].asString() << "="
           << val.asString()
           << "\n";
    }
    return ss.str();
}

bool StorageHttpDownloadHandler::downloadSSTFiles(const std::string& url,
                                          int port,
                                          const std::string& path,
                                          const std::vector<std::string>& parts,
                                          const std::string& local) {
    static folly::IOThreadPoolExecutor executor(FLAGS_download_thread_num);
    std::condition_variable cv;
    std::mutex lock;
    std::atomic<int> completed(0);
    std::atomic<bool> successful{true};

    for (auto& part : parts) {
        auto downloader = [&]() {
            auto remotePath = folly::stringPrintf("hdfs://%s:%d%s/part-%05d",
                                                  url.c_str(), port, path.c_str(),
                                                  atoi(part.c_str()));
            LOG(INFO) << "File Path " << remotePath;
            auto command = folly::stringPrintf("hdfs dfs -copyToLocal %s %s",
                                               remotePath.c_str(), local.c_str());
            LOG(INFO) << "Download SST Files: " << command;
            auto result = ProcessUtils::runCommand(command.c_str());
            if (!result.ok()) {
                LOG(ERROR) << "Failed to download SST Files: " << remotePath;
                successful = false;
            }
            std::lock_guard<std::mutex> uniqueLock(lock);
            completed++;
            cv.notify_one();
        };
        executor.add(downloader);
    }

    auto uniqueLock = std::unique_lock<std::mutex>(lock);
    cv.wait(uniqueLock, [&]() { return completed == (int32_t)parts.size(); });
    VLOG(3) << "Download tasks have finished";

    if (successful) {
        return true;
    } else {
        return false;
    }
}

bool StorageHttpDownloadHandler::ingestSSTFiles(const std::string& path,
                                        GraphSpaceID spaceID) {
    UNUSED(path); UNUSED(spaceID);
    return false;
}

}  // namespace storage
}  // namespace nebula
