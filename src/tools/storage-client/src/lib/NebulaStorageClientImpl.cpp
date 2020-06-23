/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "tools/storage-client/src/lib/NebulaStorageClientImpl.h"
#include <common/fs/FileUtils.h>

namespace nebula {

#define STDOUT_LOG "stdout.log"
#define STDERR_LOG "stderr.log"
ResultCode NebulaStorageClientImpl::initArgs(int argc, char** argv) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientImpl::setLogging(const std::string& path) {
    if (fs::FileType::DIRECTORY != fs::FileUtils::fileType(path.c_str())) {
        return ResultCode::E_DIRECTORY_NOT_FOUND;
    }
    auto regist = [&path] (const std::string &filename, FILE *stream) -> ResultCode {
        auto log = path + filename;
        auto fd = ::open(log.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0644);
        if (fd == -1) {
            return ResultCode::E_OPEN_FILE_ERROR;
        }
        if (::dup2(fd, ::fileno(stream)) == -1) {
            return ResultCode::E_OPEN_FILE_ERROR;
        }
        ::close(fd);
        return ResultCode::SUCCEEDED;
    };

    auto status = regist(STDOUT_LOG, stdout);
    if (!status) {
        return status;
    }
    return regist(STDERR_LOG, stderr);
}

ResultCode NebulaStorageClientImpl::init(const std::string& spaceName, int ioHandlers) {
    auto ret = initMetaClient(ioHandlers);
    if (ret != ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Failed to initialize meta client";
        return ret;
    }
    ret = initStorageClient();
    if (ret != ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Failed to initialize storage client";
        return ret;
    }

    auto spaceId = metaClient_->getSpaceIdByNameFromCache(spaceName);
    if (!spaceId.ok()) {
        LOG(ERROR) << "Failed get space id for " << spaceName;
        return ResultCode::E_SPACE_NOT_FOUND;
    }
    spaceId_ = spaceId.value();

    queryHandler_ = std::make_unique<NebulaStorageClientQueryImpl>(storageClient_.get(),
                                                                   metaClient_.get(),
                                                                   ioExecutor_.get(),
                                                                   spaceId_);
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientImpl::initMetaClient(int32_t ioHandlers) {
    if (metaAddr_.empty()) {
        LOG(ERROR) << "meta address is empty";
        return ResultCode::E_LOAD_META_FAILED;
    }
    ioExecutor_ = std::make_shared<folly::IOThreadPoolExecutor>(ioHandlers);
    auto addrsRet = network::NetworkUtils::toHosts(metaAddr_);
    if (!addrsRet.ok()) {
        LOG(ERROR) << "Init failed, get address failed, status " << addrsRet.status();
        return ResultCode::E_LOAD_META_FAILED;
    }
    auto addrs = std::move(addrsRet).value();
    meta::MetaClientOptions options;
    options.skipConfig_ = true;
    metaClient_ = std::make_unique<meta::MetaClient>(ioExecutor_,
                                                     std::move(addrs),
                                                     options);
    // load data try 3 time
    bool loadDataOk = metaClient_->waitForMetadReady(3);
    if (!loadDataOk) {
        LOG(ERROR) << "Failed to synchronously wait for meta service ready";
        return ResultCode::E_LOAD_META_FAILED;
    }
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientImpl::initStorageClient() {
    if (ioExecutor_ == nullptr) {
        LOG(ERROR) << "ioExecutor caused the storage client initialization failed";
        return ResultCode::E_INVALID_STORE;
    }
    if (metaClient_ == nullptr) {
        LOG(ERROR) << "Meta client caused the storage client initialization failed";
        return ResultCode::E_LOAD_META_FAILED;
    }
    storageClient_ = std::make_unique<storage::StorageClient>(ioExecutor_, metaClient_.get());
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientImpl::lookup(const LookupContext& input, ResultSet& result) {
    auto ret = queryHandler_->lookup(input);
    if (ret != ResultCode::SUCCEEDED) {
        return ret;
    }
    result = queryHandler_->resultSet();
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientImpl::getNeighbors(const NeighborsContext& input,
                                                 ResultSet& result) {
    auto ret = queryHandler_->getNeighbors(input);
    if (ret != ResultCode::SUCCEEDED) {
        return ret;
    }
    result = queryHandler_->resultSet();
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientImpl::getVertexProps(const VertexPropsContext& input,
                                                   ResultSet& result) {
    auto ret = queryHandler_->getVertexProps(input);
    if (ret != ResultCode::SUCCEEDED) {
        return ret;
    }
    result = queryHandler_->resultSet();
    return ResultCode::SUCCEEDED;
}

}   // namespace nebula


