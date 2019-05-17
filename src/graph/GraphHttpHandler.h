/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_GRAPHTTPHANDLER_H_
#define GRAPH_GRAPHTTPHANDLER_H_

#include "base/Base.h"
#include "webservice/Common.h"
#include "proxygen/httpserver/RequestHandler.h"

namespace nebula {
namespace graph {

class GraphHttpHandler : public proxygen::RequestHandler {
public:
    GraphHttpHandler() = default;

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocol) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

private:
    nebula::HttpCode err_{nebula::HttpCode::SUCCEEDED};
    std::string name;
    std::string value;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_GRAPHTTPHANDLER_H_
