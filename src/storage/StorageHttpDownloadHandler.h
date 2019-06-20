/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_STORAGEHTTPDOWNLOADHANDLER_H_
#define STORAGE_STORAGEHTTPDOWNLOADHANDLER_H_

#include "base/Base.h"
#include "webservice/Common.h"
#include "kvstore/KVStore.h"
#include "proxygen/httpserver/RequestHandler.h"

namespace nebula {
namespace storage {

using nebula::HttpCode;

class StorageHttpDownloadHandler : public proxygen::RequestHandler {
public:
    StorageHttpDownloadHandler() = default;

    void init(nebula::kvstore::KVStore *kvstore);

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body)  noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

private:
    std::string toStr(folly::dynamic& vals) const;
    bool downloadSSTFiles(const std::string& url,
                          int port,
                          const std::string& path,
                          const std::vector<std::string>& parts,
                          const std::string& local);

    bool ingestSSTFiles(const std::string& path,
                        GraphSpaceID spaceID);

private:
    HttpCode err_{HttpCode::SUCCEEDED};
    bool returnJson_{false};
    std::string hdfsUrl_;
    int hdfsPort_;
    std::string hdfsPath_;
    std::string partitions_;
    std::string localPath_;
    nebula::kvstore::KVStore *kvstore_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_STORAGEHTTPDOWNLOADHANDLER_H_
