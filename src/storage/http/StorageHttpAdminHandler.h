/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_HTTP_STORAGEHTTPADMINHANDLER_H_
#define STORAGE_HTTP_STORAGEHTTPADMINHANDLER_H_

#include "common/base/Base.h"
#include "common/webservice/Common.h"
#include "kvstore/KVStore.h"
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {
namespace storage {

using nebula::HttpCode;

class StorageHttpAdminHandler : public proxygen::RequestHandler {
public:
    StorageHttpAdminHandler(meta::SchemaManager* schemaMan, kvstore::KVStore* kv)
        : schemaMan_(schemaMan)
        , kv_(kv) {}

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body)  noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;


private:
    HttpCode err_{HttpCode::SUCCEEDED};
    std::string resp_;
    meta::SchemaManager* schemaMan_ = nullptr;
    kvstore::KVStore*    kv_ = nullptr;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_HTTP_STORAGEHTTPADMINHANDLER_H_
