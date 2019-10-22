/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <unistd.h>
#include <condition_variable>
#include <mutex>
#include <thread>
#include "nebula/NebulaClient.h"
#include "nebula/ExecuteResponse.h"

int main(int argc, char *argv[]) {
    nebula::NebulaClient::init(argc, argv);
    nebula::NebulaClient client("127.0.0.1", 3699);
    auto code = client.connect("user", "password");
    if (code != nebula::kSucceed) {
        std::cout << "Connect 127.0.0.1:3699 failed, code: " << code << std::endl;
        return 1;
    }
    // async execute cmd: show spaces
    auto cb = [&] (nebula::ExecuteResponse* response, nebula::ErrorCode resultCode) {
        if (resultCode == nebula::kSucceed) {
            std::cout << "Async do cmd \" SHOW SPACES \" succeed" << std::endl;
        } else {
            std::cout << "Async do cmd \" SHOW SPACES \" failed" << std::endl;
        }
    };

    // sync execute succeed
    client.asyncExecute("SHOW SPACES", cb);

    // sync execute failed
    client.asyncExecute("SHOW SPACE", cb);
    sleep(1);

    nebula::ExecuteResponse resp;
    code = client.execute("SHOW SPACES", resp);
    if (code != nebula::kSucceed) {
        std::cout << "Execute cmd:SHOW SPACES failed" << std::endl;
        return 1;
    }

    client.disconnect();
    return 0;
}
