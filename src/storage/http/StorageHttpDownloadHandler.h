/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_HTTP_STORAGEHTTPDOWNLOADHANDLER_H_
#define STORAGE_HTTP_STORAGEHTTPDOWNLOADHANDLER_H_

#include "common/base/Base.h"
#include "common/webservice/Common.h"
#include "common/hdfs/HdfsHelper.h"
#include "common/thread/GenericThreadPool.h"
#include "kvstore/KVStore.h"
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {
namespace storage {

using nebula::HttpCode;

class StorageHttpDownloadHandler : public proxygen::RequestHandler {
public:
    StorageHttpDownloadHandler() = default;

    void init(nebula::hdfs::HdfsHelper *helper,
              nebula::thread::GenericThreadPool *pool,
              nebula::kvstore::KVStore *kvstore,
              std::vector<std::string> paths);

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

private:
    bool downloadSSTFiles(const std::string& url,
                          int port,
                          const std::string& path,
                          const std::vector<std::string>& parts);


private:
    HttpCode err_{HttpCode::SUCCEEDED};
    GraphSpaceID spaceID_;
    std::string hdfsHost_;
    int32_t hdfsPort_;
    std::string hdfsPath_;
    std::string partitions_;
    nebula::hdfs::HdfsHelper *helper_;
    nebula::thread::GenericThreadPool *pool_;
    nebula::kvstore::KVStore *kvstore_;
    std::vector<std::string> paths_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_HTTP_STORAGEHTTPDOWNLOADHANDLER_H_
