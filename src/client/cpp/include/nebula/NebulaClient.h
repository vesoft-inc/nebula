/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_INCLUDE_NEBULACLIENT_H_
#define CLIENT_CPP_INCLUDE_NEBULACLIENT_H_


#include "ExecuteResponse.h"

namespace nebula {

class NebulaClient final {
public:
    NebulaClient(const std::string& addr,
                 uint16_t port,
                 int32_t timeout = 1000 /* ms */,
                 int16_t threadNum = 2);
    ~NebulaClient();

    // must be call on the front of the main()
    static void init(int argc, char *argv[]);

    // Authenticate the user
    ErrorCode connect(const std::string& username,
                      const std::string& password);

    void disconnect();

    // sync interface
    ErrorCode execute(std::string stmt, ExecuteResponse& resp);

    // async interface
    void asyncExecute(std::string stmt, CallbackFun cb);

private:
    std::unique_ptr<nebula::graph::NebulaClientImpl>           client_;
};

}  // namespace nebula
#endif  // CLIENT_CPP_NEBULACLIENT_H_
