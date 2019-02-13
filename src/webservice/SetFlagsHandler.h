/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef WEBSERVICE_SETFLAGSHANDLER_H_
#define WEBSERVICE_SETFLAGSHANDLER_H_

#include "base/Base.h"
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
    enum class ErrorCode {
        SUCCEEDED = 0,
        E_UNSUPPORTED_METHOD = -1,
        E_UNPROCESSABLE = -2,
    };

    ErrorCode err_{ErrorCode::SUCCEEDED};
    std::string name_;
    std::string value_;
};

}  // namespace nebula
#endif  // WEBSERVICE_SETFLAGSHANDLER_H_

