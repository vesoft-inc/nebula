/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_WEBSERVICE_GETFLAGSHANDLER_H_
#define COMMON_WEBSERVICE_GETFLAGSHANDLER_H_

#include "common/base/Base.h"
#include "common/webservice/Common.h"
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
    std::string toStr(const folly::dynamic& vals);

    static std::string valToString(const folly::dynamic& val);

private:
    HttpCode err_{HttpCode::SUCCEEDED};
    bool verbose_{false};
    bool returnJson_{false};
    std::vector<std::string> flagnames_;
};

}  // namespace nebula
#endif  // COMMON_WEBSERVICE_GETFLAGSHANDLER_H_
