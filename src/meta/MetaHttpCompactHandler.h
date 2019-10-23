/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAHTTPCOMPACTHANDLER_H
#define META_METAHTTPCOMPACTHANDLER_H

#include "base/Base.h"
#include "webservice/Common.h"
#include "kvstore/KVStore.h"
#include "thread/GenericThreadPool.h"
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {
namespace meta {

using nebula::HttpCode;

class MetaHttpCompactHandler : public proxygen::RequestHandler {
public:
    MetaHttpCompactHandler() = default;

    void init(nebula::kvstore::KVStore *kvstore,
              nebula::thread::GenericThreadPool *pool);

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

    bool compact(const std::string& spaceName, GraphSpaceID spaceId);

private:
    HttpCode err_{HttpCode::SUCCEEDED};
    GraphSpaceID spaceId_;
    std::string spaceName_;
    nebula::kvstore::KVStore *kvstore_;
    nebula::thread::GenericThreadPool *pool_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METAHTTPINGESTHANDLER_H

