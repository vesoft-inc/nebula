/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_
#define CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_

#include "client/cpp/include/nebula/ExecutionResponse.h"
#include "ConnectionThread.h"
#include "gen-cpp2/GraphServiceAsyncClient.h"
#include <string>

namespace nebula {
namespace graph {

class NebulaClientImpl final {
public:
    explicit NebulaClientImpl(const std::string &addr, const uint32_t port)
        : addr_(addr)
        , port_(port) {}
    ~NebulaClientImpl();

    // must be call on the front of the main()
    static void initEnv(int argc, char *argv[]);
    static void initConnectionPool(const std::vector<ConnectionInfo> &addrInfo);

    // Authenticate the user
    ErrorCode doAuthenticate(const std::string& username,
                             const std::string& password);

    cpp2::ErrorCode authenticate(const std::string& username,
                                 const std::string& password);
    void signout();

    cpp2::ErrorCode execute(folly::StringPiece stmt,
                            cpp2::ExecutionResponse& resp);

    ErrorCode doExecute(std::string stmt, ExecutionResponse& resp);

    void doAsyncExecute(std::string stmt, CallbackFun cb);

private:
    void feedPath(const cpp2::Path &inPath, Path& outPath);

    void feedRows(const cpp2::ExecutionResponse& inResp,
                  ExecutionResponse& outResp);

private:
    ConnectionThread*                   connection_{nullptr};
    int32_t                             indexId_{-1};
    std::string                         addr_;
    uint32_t                            port_;
};

}  // namespace graph
}  // namespace nebula
#endif  // CLIENT_CPP_LIB_NEBULACLIENTIMPL_H_
