/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_HTTP_STORAGEHTTPSTATUSHANDLER_H_
#define STORAGE_HTTP_STORAGEHTTPSTATUSHANDLER_H_

#include "base/Base.h"
#include "webservice/Common.h"
#include "kvstore/KVStore.h"
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {
namespace storage {

using nebula::HttpCode;

class StorageHttpStatusHandler : public proxygen::RequestHandler {
public:
    StorageHttpStatusHandler() = default;

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body)  noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

private:
    void addOneStatus(folly::dynamic& vals,
                      const std::string& statusName,
                      const std::string& statusValue) const;

    std::string readValue(std::string& statusName);
    void readAllValue(folly::dynamic& vals);
    folly::dynamic getStatus();
    std::string toStr(folly::dynamic& vals) const;

private:
    HttpCode err_{HttpCode::SUCCEEDED};
    bool returnJson_{false};
    std::vector<std::string> statusNames_;
    std::vector<std::string> statusAllNames_{"status"};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_HTTP_STORAGEHTTPSTATUSHANDLER_H_
