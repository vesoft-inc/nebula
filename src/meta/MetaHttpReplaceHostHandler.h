/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAHTTPREPLACEHOSTHANDLER_H
#define META_METAHTTPREPLACEHOSTHANDLER_H

#include "base/Base.h"
#include "webservice/Common.h"
#include "kvstore/KVStore.h"
#include "thread/GenericThreadPool.h"
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {
namespace meta {

using nebula::HttpCode;

class MetaHttpReplaceHostHandler : public proxygen::RequestHandler {
    FRIEND_TEST(MetaHttpReplaceHandlerTest, FooTest);

public:
    MetaHttpReplaceHostHandler() = default;

    void init(nebula::kvstore::KVStore *kvstore);

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

    bool replaceHost(IPv4 ipv4From, IPv4 ipv4To);

private:
    HttpCode err_{HttpCode::SUCCEEDED};
    std::string errMsg_;
    IPv4 ipv4From_;
    IPv4 ipv4To_;
    nebula::kvstore::KVStore *kvstore_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METAHTTPReplaceHostHANDLER_H

