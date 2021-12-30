/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef GRAPH_SERVICE_GRAPHSERVICE_H_
#define GRAPH_SERVICE_GRAPHSERVICE_H_

#include "common/base/Base.h"
#include "graph/service/Authenticator.h"
#include "graph/service/QueryEngine.h"
#include "graph/session/GraphSessionManager.h"
#include "interface/gen-cpp2/GraphService.h"

namespace folly {
class IOThreadPoolExecutor;
}  // namespace folly

namespace nebula {
namespace graph {

class GraphService final : public cpp2::GraphServiceSvIf {
 public:
  GraphService() = default;
  ~GraphService() = default;

  Status NG_MUST_USE_RESULT init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor,
                                 const HostAddr& hostAddr);

  folly::Future<AuthResponse> future_authenticate(const std::string& username,
                                                  const std::string& password) override;

  void signout(int64_t /*sessionId*/) override;

  folly::Future<ExecutionResponse> future_executeWithParameter(
      int64_t sessionId,
      const std::string& stmt,
      const std::unordered_map<std::string, Value>& parameterMap) override;

  folly::Future<ExecutionResponse> future_execute(int64_t sessionId,
                                                  const std::string& stmt) override;

  folly::Future<std::string> future_executeJsonWithParameter(
      int64_t sessionId,
      const std::string& stmt,
      const std::unordered_map<std::string, Value>& parameterMap) override;

  folly::Future<std::string> future_executeJson(int64_t sessionId,
                                                const std::string& stmt) override;

  folly::Future<cpp2::VerifyClientVersionResp> future_verifyClientVersion(
      const cpp2::VerifyClientVersionReq& req) override;

  std::unique_ptr<meta::MetaClient> metaClient_;

 private:
  Status auth(const std::string& username, const std::string& password);

  std::unique_ptr<GraphSessionManager> sessionManager_;
  std::unique_ptr<QueryEngine> queryEngine_;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_GRAPHSERVICE_H_
