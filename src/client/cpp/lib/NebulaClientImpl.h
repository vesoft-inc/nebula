/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_
#define CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_

#include "client/cpp/include/nebula/ExecuteResponse.h"
#include "ConnectionThread.h"
#include "gen-cpp2/GraphServiceAsyncClient.h"
#include <string>

namespace nebula {
namespace graph {

class NebulaClientImpl final {
public:
    NebulaClientImpl() = default;
    ~NebulaClientImpl();

    // must be call on the front of the main()
    static void initEnv(int argc, char *argv[]);
    static void initSocketPool(const std::string& addr,
                               uint16_t port,
                               int32_t timeout = 1000,
                               int32_t socketNum = 1);

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
    ConnectionThread*                   connection_{nullptr};
    int32_t                             connectionId_;
};

}  // namespace graph
}  // namespace nebula
#endif  // CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_
