/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef WEBSERVICE_SETFLAGSHANDLER_H_
#define WEBSERVICE_SETFLAGSHANDLER_H_

#include "base/Base.h"
#include "webservice/Common.h"
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {

class SetFlagsHandler : public proxygen::RequestHandler {
public:
    SetFlagsHandler() = default;

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers)
        noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError err) noexcept override;

private:
    HttpCode err_{HttpCode::SUCCEEDED};
    std::string name_;
    std::string value_;
};

}  // namespace nebula
#endif  // WEBSERVICE_SETFLAGSHANDLER_H_

