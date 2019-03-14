/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef WEBSERVICE_GETFLAGSHANDLER_H_
#define WEBSERVICE_GETFLAGSHANDLER_H_

#include "base/Base.h"
#include "webservice/Common.h"
#include <folly/dynamic.h>
#include <proxygen/httpserver/RequestHandler.h>

namespace nebula {

class GetFlagsHandler : public proxygen::RequestHandler {
public:
    GetFlagsHandler() = default;

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers)
        noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol proto) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError err) noexcept override;

private:
    folly::dynamic getFlags();
    void addOneFlag(folly::dynamic& vals,
                    const std::string& flagname,
                    const gflags::CommandLineFlagInfo* info);
    std::string toStr(folly::dynamic& vals);

private:
    HttpCode err_{HttpCode::SUCCEEDED};
    bool verbose_{false};
    bool returnJson_{false};
    std::vector<std::string> flagnames_;
};

}  // namespace nebula
#endif  // WEBSERVICE_GETFLAGSHANDLER_H_

