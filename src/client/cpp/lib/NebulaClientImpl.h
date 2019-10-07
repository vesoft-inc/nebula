/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_
#define CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_

#include "client/cpp/include/nebula/ExecuteResponse.h"
#include "gen-cpp2/GraphServiceAsyncClient.h"
#include <string>

namespace nebula {
namespace graph {

class NebulaClientImpl {
public:
    NebulaClientImpl(const std::string& addr, uint16_t port);
    virtual ~NebulaClientImpl();

    // must be call on the front of the main()
    static void initEnv(int argc, char *argv[]);

    // Authenticate the user
    ErrorCode doConnect(const std::string& username,
                        const std::string& password);

    cpp2::ErrorCode connect(const std::string& username,
                            const std::string& password);
    void disconnect();

    cpp2::ErrorCode execute(folly::StringPiece stmt,
                            cpp2::ExecutionResponse& resp);

    ErrorCode doExecute(std::string stmt,
                        ExecuteResponse& resp);

private:
    void feedRows(const cpp2::ExecutionResponse& inResp,
                  ExecuteResponse& outResp);

private:
    using GraphClient = std::unique_ptr<nebula::graph::cpp2::GraphServiceAsyncClient>;
    GraphClient                   client_;
    const std::string             addr_;
    const uint16_t                port_;
    int64_t                       sessionId_;
};

}  // namespace graph
}  // namespace nebula
#endif  // CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_
