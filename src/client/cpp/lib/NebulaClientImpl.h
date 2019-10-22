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
#include <folly/executors/IOThreadPoolExecutor.h>

namespace nebula {
namespace graph {

class NebulaClientImpl final {
public:
    NebulaClientImpl(const std::string& addr,
                     uint16_t port,
                     int32_t timeout = 1000,
                     int16_t threadNum = 2);
    ~NebulaClientImpl();

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

    ErrorCode doExecute(std::string stmt, ExecuteResponse& resp);

    void doAsyncExecute(std::string stmt, CallbackFun cb);

private:
    void feedPath(const cpp2::Path &inPath, Path& outPath);

    void feedRows(const cpp2::ExecutionResponse& inResp,
                  ExecuteResponse& outResp);

private:
    using GraphClient = std::unique_ptr<cpp2::GraphServiceAsyncClient>;
    GraphClient                                      client_;
    std::shared_ptr<folly::IOThreadPoolExecutor>     ioThreadPool_;
    const std::string                                addr_;
    const uint16_t                                   port_;
    int64_t                                          sessionId_;
    int32_t                                          timeout_;  //  In milliseconds
    int16_t                                          threadNum_;
};

}  // namespace graph
}  // namespace nebula
#endif  // CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_
