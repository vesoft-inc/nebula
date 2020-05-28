/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_HTTP_STORAGEHTTPINGESTHANDLER_H_
#define STORAGE_HTTP_STORAGEHTTPINGESTHANDLER_H_

#include "common/base/Base.h"
#include "common/webservice/Common.h"
#include "kvstore/KVStore.h"
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {
namespace storage {

using nebula::HttpCode;

class StorageHttpIngestHandler : public proxygen::RequestHandler {
public:
    StorageHttpIngestHandler() = default;

    void init(nebula::kvstore::KVStore *kvstore);

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body)  noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

    bool ingestSSTFiles(GraphSpaceID space);

private:
    HttpCode err_{HttpCode::SUCCEEDED};
    nebula::kvstore::KVStore *kvstore_;
    GraphSpaceID space_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_HTTP_STORAGEHTTPINGESTHANDLER_H_

