/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef GRAPH_GRAPHTTPHANDLER_H_
#define GRAPH_GRAPHTTPHANDLER_H_

#include "base/Base.h"
#include "network/NetworkUtils.h"
#include "proxygen/httpserver/RequestHandler.h"

namespace nebula {
namespace graph {

using nebula::network::NetworkUtils;

class GraphHttpHandler : public proxygen::RequestHandler {
public:
    GraphHttpHandler() = default;

    void onRequest(std::unique_ptr<proxygen::HTTPMessage> headers) noexcept override;

    void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override;

    void onEOM() noexcept override;

    void onUpgrade(proxygen::UpgradeProtocol protocal) noexcept override;

    void requestComplete() noexcept override;

    void onError(proxygen::ProxygenError error) noexcept override;

private:
    NetworkUtils::Code err_{NetworkUtils::Code::SUCCEEDED};
    std::string name;
    std::string value;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_GRAPHTTPHANDLER_H_
