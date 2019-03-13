/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef WEBSERVICE_GETSTATSHANDLER_H_
#define WEBSERVICE_GETSTATSHANDLER_H_

#include "base/Base.h"
#include <folly/dynamic.h>
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {

class GetStatsHandler : public proxygen::RequestHandler {
public:
    GetStatsHandler() = default;

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers)
        noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError err) noexcept override;

private:
    folly::dynamic getStats();
    void addOneStats(folly::dynamic& vals, const std::string& statName,
                     int64_t statValue);
    std::string toStr(folly::dynamic& vals);

private:
    enum class ErrorCode {
        SUCCEEDED = 0,
        E_UNSUPPORTED_METHOD = -1,
    };

    ErrorCode err_{ErrorCode::SUCCEEDED};
    bool returnJson_{false};
    std::vector<std::string> statnames_;
};

}  // namespace nebula
#endif  // WEBSERVICE_GETFLAGSHANDLER_H_

