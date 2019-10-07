/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef CLIENT_CPP_INCLUDE_NEBULACLIENT_H_
#define CLIENT_CPP_INCLUDE_NEBULACLIENT_H_


#include "ExecuteResponse.h"

namespace nebula {
namespace graph {

class NebulaClientImpl;


class NebulaClient {
public:
    NebulaClient(const std::string& addr, uint16_t port);
    virtual ~NebulaClient();

    // must be call on the front of the main()
    static void init(int argc, char *argv[]);

    // Authenticate the user
    ErrorCode connect(const std::string& username,
                   const std::string& password);
    void disconnect();

    ErrorCode execute(std::string stmt, ExecuteResponse& resp);

private:
    std::unique_ptr<NebulaClientImpl> client_;
    const std::string addr_;
    const uint16_t port_;
};

}  // namespace graph
}  // namespace nebula
#endif  // CLIENT_CPP_NEBULACLIENT_H_
