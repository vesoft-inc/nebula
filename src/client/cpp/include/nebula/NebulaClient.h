/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_INCLUDE_NEBULACLIENT_H_
#define CLIENT_CPP_INCLUDE_NEBULACLIENT_H_

#include <vector>
#include "ExecutionResponse.h"

namespace nebula {

// Compatible with GCC 4.8
typedef void(*ExecCallback)(ExecutionResponse*, ErrorCode, void *);

class NebulaClient final {
public:
    explicit NebulaClient(const std::string &addr, const uint32_t port);
    ~NebulaClient();

    // must be call on the front of the main()
    static void init(int argc, char *argv[]);
    static void initConnectionPool(const std::vector<ConnectionInfo> &addrInfo);
    // Authenticate the user
    ErrorCode authenticate(const std::string& username,
                           const std::string& password);

    void signout();

    // sync interface
    ErrorCode execute(std::string stmt, ExecutionResponse& resp);

    // async interface
    void asyncExecute(std::string stmt, ExecCallback cb, void *context);

private:
    std::unique_ptr<nebula::graph::NebulaClientImpl>           client_;
};

}  // namespace nebula
#endif  // CLIENT_CPP_NEBULACLIENT_H_
