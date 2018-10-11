/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef SERVER_GRAPHDBSERVICEHANDLER_H_
#define SERVER_GRAPHDBSERVICEHANDLER_H_

#include "base/Base.h"
#include "interface/gen-cpp2/GraphDbService.h"

namespace vesoft {
namespace vgraph {

class GraphDbServiceHandler final : public cpp2::GraphDbServiceSvIf {
public:
    folly::Future<cpp2::AuthResponse> future_authenticate(
        const std::string& username,
        const std::string& password) override;

    void signout(int64_t /*sessionId*/) override;

    folly::Future<cpp2::ExecutionResponse> future_execute(
        int64_t sessionId,
        const std::string& stmt) override;

    const char* getErrorStr(cpp2::ErrorCode result);

private:
};

}  // namespace vgraph
}  // namespace vesoft
#endif  // SERVER_GRAPHDBSERVICEHANDLER_H_
