/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WEBSERVICE_SYSINFOHANDLER_H_
#define WEBSERVICE_SYSINFOHANDLER_H_

#include "base/Base.h"
#include "webservice/Common.h"
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {
namespace storage {

using nebula::HttpCode;

class SysInfoHandler : public proxygen::RequestHandler {
public:
    SysInfoHandler() = default;

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body)  noexcept override;

    void init(std::string data_path);

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

private:
    folly::dynamic getSysInfo();

private:
    HttpCode err_{HttpCode::SUCCEEDED};
    std::string data_path_;
};

}  // namespace storage
}  // namespace nebula

#endif  // WEBSERVICE_SYSINFOHANDLER_H_
