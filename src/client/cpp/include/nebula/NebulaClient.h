/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_INCLUDE_NEBULACLIENT_H_
#define CLIENT_CPP_INCLUDE_NEBULACLIENT_H_


#include "ExecutionResponse.h"

namespace nebula {

class NebulaClient final {
public:
    NebulaClient();
    ~NebulaClient();

    // must be call on the front of the main()
    static void init(int argc, char *argv[]);
    static void initConnectionPool(const std::string& addr,
                                   uint16_t port,
                                   uint16_t connectionNum = 10,
                                   int32_t timeout = 1000 /* ms */);
    // Authenticate the user
    ErrorCode authenticate(const std::string& username,
                           const std::string& password);

    void signout();

    // sync interface
    ErrorCode execute(std::string stmt, ExecutionResponse& resp);

    // async interface
    void asyncExecute(std::string stmt, CallbackFun cb);

private:
    std::unique_ptr<nebula::graph::NebulaClientImpl>           client_;
};

}  // namespace nebula
#endif  // CLIENT_CPP_NEBULACLIENT_H_
