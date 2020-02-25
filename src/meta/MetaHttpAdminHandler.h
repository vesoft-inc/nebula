/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_METAHTTPADMINHANDLER_H_
#define META_METAHTTPADMINHANDLER_H_

#include "base/Base.h"
#include "webservice/Common.h"
#include "kvstore/KVStore.h"
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {
namespace meta {

using nebula::HttpCode;

class MetaHttpAdminHandler : public proxygen::RequestHandler {
public:
    MetaHttpAdminHandler() = default;

    void init(nebula::kvstore::KVStore *kvstore);

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

private:
    HttpCode err_{HttpCode::SUCCEEDED};
    std::string resp_;
    nebula::kvstore::KVStore *kvstore_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_METAHTTPADMINHANDLER_H_
