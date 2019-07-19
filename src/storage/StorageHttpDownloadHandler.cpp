/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/StorageHttpDownloadHandler.h"
#include "webservice/Common.h"
#include "process/ProcessUtils.h"
#include "hdfs/HdfsHelper.h"
#include "thread/GenericThreadPool.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <mutex>

DEFINE_int32(download_thread_num, 3, "download thread number");

namespace nebula {
namespace storage {

static std::atomic_flag isRunning = ATOMIC_FLAG_INIT;
std::once_flag poolStartFlag;

using proxygen::HTTPMessage;
using proxygen::HTTPMethod;
using proxygen::ProxygenError;
using proxygen::UpgradeProtocol;
using proxygen::ResponseBuilder;

void StorageHttpDownloadHandler::init(nebula::hdfs::HdfsHelper *helper) {
    helper_ = helper;
    CHECK_NOTNULL(helper_);
}

void StorageHttpDownloadHandler::onRequest(std::unique_ptr<HTTPMessage> headers) noexcept {
    if (headers->getMethod().value() != HTTPMethod::GET) {
        // Unsupported method
        err_ = HttpCode::E_UNSUPPORTED_METHOD;
        return;
    }

     if (!headers->hasQueryParam("host") ||
         !headers->hasQueryParam("port") ||
         !headers->hasQueryParam("path") ||
         !headers->hasQueryParam("parts") ||
         !headers->hasQueryParam("local")) {
         LOG(ERROR) << "Illegal Argument";
         err_ = HttpCode::E_ILLEGAL_ARGUMENT;
         return;
     }
     hdfsHost_ = headers->getQueryParam("host");
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

    if (helper_->checkHadoopPath()) {
        std::vector<std::string> parts;
        folly::split(",", partitions_, parts, true);
        if (parts.size() == 0) {
            ResponseBuilder(downstream_)
                .status(400, "SSTFile download failed")
                .body("Partitions should be not empty")
                .sendWithEOM();
        }

        if (downloadSSTFiles(hdfsHost_, hdfsPort_, hdfsPath_, parts, localPath_)) {
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
        LOG(ERROR) << "HADOOP_HOME not exist";
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
    LOG(ERROR) << "Web Service StorageHttpDownloadHandler got error: "
               << proxygen::getErrorString(error);
}

bool StorageHttpDownloadHandler::downloadSSTFiles(const std::string& hdfsHost,
                                                  int32_t hdfsPort,
                                                  const std::string& hdfsPath,
                                                  const std::vector<std::string>& parts,
                                                  const std::string& localPath) {
    if (isRunning.test_and_set()) {
        LOG(ERROR) << "Download is not completed";
        return false;
    }

    std::vector<folly::SemiFuture<bool>> futures;
    static nebula::thread::GenericThreadPool pool;
    std::call_once(poolStartFlag, []() {
        LOG(INFO) << "Download Thread Pool start";
        pool.start(FLAGS_download_thread_num);
    });

    for (auto& part : parts) {
        auto downloader = [hdfsHost, hdfsPort, hdfsPath, localPath, part, this]() {
            int32_t partInt;
            try {
                partInt = folly::to<int32_t>(part);
            } catch (const std::exception& ex) {
                LOG(ERROR) << "Invalid part: \"" << part << "\"";
                return false;
            }

            auto hdfsPartPath = folly::stringPrintf("%s/%d", hdfsPath.c_str(), partInt);
            auto result = this->helper_->copyToLocal(hdfsHost, hdfsPort,
                                                     hdfsPartPath, localPath);
            if (!result.ok() || !result.value().empty()) {
                LOG(ERROR) << "Download SSTFile Failed";
                return false;
            } else {
                return true;
            }
        };
        auto future = pool.addTask(downloader);
        futures.push_back(std::move(future));
    }

    std::atomic<bool> successfully{true};
    folly::collectAll(futures).then([&](const std::vector<folly::Try<bool>>& tries) {
        for (const auto& t : tries) {
            if (t.hasException()) {
                LOG(ERROR) << "Download Failed: " << t.exception();
                successfully = false;
                break;
            }
            if (!t.value()) {
                successfully = false;
                break;
            }
        }
    }).wait();
    LOG(INFO) << "Download tasks have finished";
    isRunning.clear();
    return successfully;
}

}  // namespace storage
}  // namespace nebula
