/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAHTTPDOWNLOADHANDLER_H_
#define META_METAHTTPDOWNLOADHANDLER_H_

#include "base/Base.h"
#include "webservice/Common.h"
#include "kvstore/KVStore.h"
#include "hdfs/HdfsHelper.h"
#include "thread/GenericThreadPool.h"
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {
namespace meta {

using nebula::HttpCode;

class MetaHttpDownloadHandler : public proxygen::RequestHandler {
public:
    MetaHttpDownloadHandler() = default;

    void init(nebula::kvstore::KVStore *kvstore,
              nebula::hdfs::HdfsHelper *helper,
              nebula::thread::GenericThreadPool *pool);

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

private:
    bool dispatchSSTFiles();

private:
    HttpCode err_{HttpCode::SUCCEEDED};
    std::string hdfsHost_;
    int32_t hdfsPort_;
    std::string hdfsPath_;
    GraphSpaceID spaceID_;
    folly::Optional<TagID> tag_;
    folly::Optional<EdgeType> edge_;
    nebula::kvstore::KVStore *kvstore_;
    nebula::hdfs::HdfsHelper *helper_;
    nebula::thread::GenericThreadPool *pool_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METAHTTPDOWNLOADHANDLER_H_
