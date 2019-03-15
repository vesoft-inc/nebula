/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_STORAGEHTTPHANDLER_H_
#define STORAGE_STORAGEHTTPHANDLER_H_

#include "base/Base.h"
#include "webservice/Common.h"
#include "proxygen/httpserver/RequestHandler.h"

namespace nebula {
namespace storage {

class StorageHttpHandler : public proxygen::RequestHandler {
public:
    StorageHttpHandler() = default;

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body)  noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

private:
    nebula::HttpCode code{nebula::HttpCode::SUCCEEDED};
    std::string name;
    std::string value;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_STORAGEHTTPHANDLER_H_
