/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/http/StorageHttpDownloadHandler.h"
#include "common/webservice/Common.h"
#include "common/process/ProcessUtils.h"
#include "common/fs/FileUtils.h"
#include "common/hdfs/HdfsHelper.h"
#include "kvstore/Part.h"
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

void StorageHttpDownloadHandler::init(nebula::hdfs::HdfsHelper *helper,
                                      nebula::thread::GenericThreadPool *pool,
                                      nebula::kvstore::KVStore *kvstore,
                                      std::vector<std::string> paths) {
    helper_ = helper;
    pool_ = pool;
    kvstore_ = kvstore;
    paths_ = paths;
    CHECK_NOTNULL(helper_);
    CHECK_NOTNULL(pool_);
    CHECK_NOTNULL(kvstore_);
    CHECK(!paths_.empty());
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
         !headers->hasQueryParam("space")) {
         LOG(ERROR) << "Illegal Argument";
         err_ = HttpCode::E_ILLEGAL_ARGUMENT;
         return;
     }

     hdfsHost_ = headers->getQueryParam("host");
     hdfsPort_ = headers->getIntQueryParam("port");
     hdfsPath_ = headers->getQueryParam("path");
     partitions_ = headers->getQueryParam("parts");
     spaceID_ = headers->getIntQueryParam("space");

     for (auto &path : paths_) {
         auto downloadPath = folly::stringPrintf("%s/nebula/%d/download", path.c_str(), spaceID_);
         if (fs::FileUtils::fileType(downloadPath.c_str()) == fs::FileType::NOTEXIST) {
             fs::FileUtils::makeDir(downloadPath);
         }
     }
}


void StorageHttpDownloadHandler::onBody(std::unique_ptr<folly::IOBuf>) noexcept {
    // Do nothing, we only support GET
}


void StorageHttpDownloadHandler::onEOM() noexcept {
    switch (err_) {
        case HttpCode::E_UNSUPPORTED_METHOD:
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::METHOD_NOT_ALLOWED),
                        WebServiceUtils::toString(HttpStatusCode::METHOD_NOT_ALLOWED))
                .sendWithEOM();
            return;
        case HttpCode::E_ILLEGAL_ARGUMENT:
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::BAD_REQUEST),
                        WebServiceUtils::toString(HttpStatusCode::BAD_REQUEST))
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

        if (downloadSSTFiles(hdfsHost_, hdfsPort_, hdfsPath_, parts)) {
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::OK),
                        WebServiceUtils::toString(HttpStatusCode::OK))
                .body("SSTFile download successfully")
                .sendWithEOM();
        } else {
            ResponseBuilder(downstream_)
                .status(WebServiceUtils::to(HttpStatusCode::FORBIDDEN),
                        WebServiceUtils::toString(HttpStatusCode::FORBIDDEN))
                .body("SSTFile download failed")
                .sendWithEOM();
        }
    } else {
        LOG(ERROR) << "HADOOP_HOME not exist";
        ResponseBuilder(downstream_)
            .status(WebServiceUtils::to(HttpStatusCode::NOT_FOUND),
                    WebServiceUtils::toString(HttpStatusCode::NOT_FOUND))
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
                                                  const std::vector<std::string>& parts) {
    static std::atomic_flag isRunning = ATOMIC_FLAG_INIT;
    if (isRunning.test_and_set()) {
        LOG(ERROR) << "Download is not completed";
        return false;
    }

    std::vector<folly::SemiFuture<bool>> futures;

    for (auto& part : parts) {
        PartitionID partId;
        try {
            partId = folly::to<PartitionID>(part);
        } catch (const std::exception& ex) {
            isRunning.clear();
            LOG(ERROR) << "Invalid part: \"" << part << "\"";
            return false;
        }

        auto downloader = [hdfsHost, hdfsPort, hdfsPath, partId, this]() {
            auto hdfsPartPath = folly::stringPrintf("%s/%d", hdfsPath.c_str(), partId);
            auto partResult = kvstore_->part(spaceID_, partId);
            if (!ok(partResult)) {
                LOG(ERROR) << "Can't found space: " << spaceID_ << ", part: " << partId;
                return false;
            }

            auto localPath = folly::stringPrintf("%s/download/",
                                                 value(partResult)->engine()->getDataRoot());
            auto result = this->helper_->copyToLocal(hdfsHost, hdfsPort,
                                                     hdfsPartPath, localPath);
            return result.ok() && result.value().empty();
        };
        auto future = pool_->addTask(downloader);
        futures.push_back(std::move(future));
    }

    bool successfully{true};
    auto tries = folly::collectAll(futures).get();
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
    LOG(INFO) << "Download tasks have finished";
    isRunning.clear();
    return successfully;
}

}  // namespace storage
}  // namespace nebula
