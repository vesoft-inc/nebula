/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef GRAPH_GRAPHSERVICE_H_
#define GRAPH_GRAPHSERVICE_H_

#include "base/Base.h"
#include "gen-cpp2/GraphService.h"
#include "graph/PasswordAuthenticator.h"
#include "graph/CloudAuthenticator.h"
#include "graph/ExecutionEngine.h"
#include "graph/SessionManager.h"

namespace folly {
class IOThreadPoolExecutor;
}   // namespace folly

namespace nebula {
namespace graph {

class GraphService final : public cpp2::GraphServiceSvIf {
public:
    GraphService() = default;
    ~GraphService() = default;

    Status MUST_USE_RESULT init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor);

    folly::Future<cpp2::AuthResponse> future_authenticate(
        const std::string& username,
        const std::string& password) override;

    void signout(int64_t /*sessionId*/) override;

    folly::Future<cpp2::ExecutionResponse>
    future_execute(int64_t sessionId, const std::string& stmt) override;

    const char* getErrorStr(cpp2::ErrorCode result);

private:
    void onHandle(RequestContext<cpp2::AuthResponse>& ctx, cpp2::ErrorCode code);

    session::Role toRole(nebula::cpp2::RoleType role);

    bool auth(const std::string& username, const std::string& password);

private:
    std::unique_ptr<SessionManager>             sessionManager_;
    std::unique_ptr<ExecutionEngine>            executionEngine_;
    std::unique_ptr<meta::MetaClient>           metaClient_;
};

}   // namespace graph
}   // namespace nebula
#endif   // GRAPH_GRAPHSERVICE_H_
