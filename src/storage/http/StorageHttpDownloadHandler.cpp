/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/http/StorageHttpDownloadHandler.h"
#include "webservice/Common.h"
#include "process/ProcessUtils.h"
#include "fs/FileUtils.h"
#include "hdfs/HdfsHelper.h"
#include "kvstore/Part.h"
#include "thread/GenericThreadPool.h"
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/lib/http/ProxygenErrorEnum.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <mutex>

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
    auto existStatus = helper_->exist(hdfsHost_, hdfsPort_, hdfsPath_);
    if (!existStatus.ok()) {
        LOG(ERROR) << "Run Hdfs Test failed. " << existStatus.status().toString();
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
    }
    bool exist = existStatus.value();
    if (!exist) {
        LOG(ERROR) << "Hdfs non exist. hdfs://" << hdfsHost_ << ":" << hdfsPort_ << hdfsPath_;
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }
    spaceID_ = headers->getIntQueryParam("space");
    auto partitions = headers->getQueryParam("parts");
    folly::split(",", partitions, parts_, true);
    if (parts_.empty()) {
        LOG(ERROR) << "Partitions should be not empty";
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }
    try {
        if (headers->hasQueryParam("tag")) {
            auto& tag = headers->getQueryParam("tag");
            tag_.assign(folly::to<TagID>(tag));
        }
        if (headers->hasQueryParam("edge")) {
            auto& edge = headers->getQueryParam("edge");
            edge_.assign(folly::to<EdgeType>(edge));
        }
    } catch (std::exception& e) {
        LOG(ERROR) << "Parse tag/edge error. " << e.what();
        err_ = HttpCode::E_ILLEGAL_ARGUMENT;
        return;
    }
    for (auto &path : paths_) {
        std::string downloadRootPath = folly::stringPrintf(
            "%s/nebula/%d/download", path.c_str(), spaceID_);
        std::string downloadRootPathEdge = downloadRootPath + "/edge";
        std::string downloadRootPathTag = downloadRootPath + "/tag";
        std::string downloadRootPathGeneral = downloadRootPath + "/general";

        std::string downloadPath;
        if (edge_.has_value()) {
            downloadPath = folly::stringPrintf(
                "%s/%d",
                downloadRootPathEdge.c_str(), edge_.value());
        } else if (tag_.has_value()) {
            downloadPath = folly::stringPrintf(
                "%s/%d",
                downloadRootPathTag.c_str(), tag_.value());
        } else {
            downloadPath = downloadRootPathGeneral;
        }
        fs::FileUtils::remove(downloadPath.c_str(), true);
        fs::FileUtils::makeDir(downloadPath);
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
        if (downloadSSTFiles()) {
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

bool StorageHttpDownloadHandler::downloadSSTFiles() {
    std::vector<folly::SemiFuture<bool>> futures;
    for (auto& part : parts_) {
        PartitionID partId;
        try {
            partId = folly::to<PartitionID>(part);
        } catch (const std::exception& ex) {
            LOG(ERROR) << "Invalid part: \"" << part << "\"" << ", error: " << ex.what();
            return false;
        }
        auto hdfsPartPath = folly::stringPrintf("%s/%d", hdfsPath_.c_str(), partId);
        auto existStatus = helper_->exist(hdfsHost_, hdfsPort_, hdfsPartPath);
        if (!existStatus.ok()) {
            LOG(ERROR) << "Run Hdfs Test failed. " << existStatus.status().toString();
            return false;
        }
        bool exist = existStatus.value();
        if (!exist) {
            LOG(WARNING) << "Hdfs path non exist. hdfs://"
                         << hdfsHost_ << ":" << hdfsPort_ << hdfsPartPath;
            continue;
        }
        auto partResult = kvstore_->part(spaceID_, partId);
        if (!ok(partResult)) {
            LOG(ERROR) << "Can't found space: " << spaceID_ << ", part: " << partId;
            return false;
        }
        auto partDataRoot = value(partResult)->engine()->getDataRoot();
        std::string localPath;
        if (edge_.has_value()) {
            localPath = folly::stringPrintf(
                "%s/download/edge/%d", partDataRoot, edge_.value());
        } else if (tag_.has_value()) {
            localPath = folly::stringPrintf(
                "%s/download/tag/%d", partDataRoot, tag_.value());
        } else {
            localPath = folly::stringPrintf(
                "%s/download/general", partDataRoot);
        }
        auto downloader = [this, hdfsPartPath, localPath] {
            auto resultStatus = this->helper_->copyToLocal(
                hdfsHost_, hdfsPort_, hdfsPartPath, localPath);
            if (!resultStatus.ok()) {
                LOG(ERROR) << "Run Hdfs CopyToLocal failed. "
                           << resultStatus.status().toString();
                return false;
            }
            auto result = std::move(resultStatus).value();
            return result.empty();
        };
        auto future = pool_->addTask(downloader);
        futures.push_back(std::move(future));
    }
    bool successfully{true};
    folly::collectAll(futures).thenValue([&](const std::vector<folly::Try<bool>>& tries) {
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
    return successfully;
}

}  // namespace storage
}  // namespace nebula
