/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "nebula/NebulaClient.h"
#include "nebula/ExecuteResponse.h"

int main(int argc, char *argv[]) {
    nebula::graph::NebulaClient::init(argc, argv);
    nebula::graph::NebulaClient client("127.0.0.1", 3699);
    auto code = client.connect("user", "password");
    if (code != kSucceed) {
        std::cout << "Connect 127.0.0.1:3699 failed, code: " << code << std::endl;
        return 1;
    }
    // execute cmd: show spaces
    nebula::graph::ExecuteResponse resp;
    code = client.execute("SHOW SPACES", resp);
    if (code != kSucceed) {
        std::cout << "Execute cmd:SHOW SPACES failed" << std::endl;
        return 1;
    }
    return 0;
}
