/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef VGRAPHD_VGRAPHSERVICE_H_
#define VGRAPHD_VGRAPHSERVICE_H_

#include "base/Base.h"
#include "interface/gen-cpp2/GraphDbService.h"
#include "vgraphd/SessionManager.h"
#include "vgraphd/ExecutionEngine.h"
#include "vgraphd/Authenticator.h"

namespace vesoft {
namespace vgraph {

class VGraphService final : public cpp2::GraphDbServiceSvIf {
public:
    VGraphService();
    ~VGraphService();
    folly::Future<cpp2::AuthResponse> future_authenticate(
        const std::string& username,
        const std::string& password) override;

    void signout(int64_t /*sessionId*/) override;

    folly::Future<cpp2::ExecutionResponse> future_execute(
        int64_t sessionId,
        const std::string& stmt) override;

    const char* getErrorStr(cpp2::ErrorCode result);

private:
    std::unique_ptr<SessionManager>             sessionManager_;
    std::unique_ptr<ExecutionEngine>            executionEngine_;
    std::unique_ptr<Authenticator>              authenticator_;
};

}  // namespace vgraph
}  // namespace vesoft
#endif  // VGRAPHD_VGRAPHSERVICE_H_
