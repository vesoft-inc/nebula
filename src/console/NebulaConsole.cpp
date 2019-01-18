/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "console/CliManager.h"

DEFINE_string(addr, "127.0.0.1", "Nebula daemon IP address");
DEFINE_int32(port, 34500, "Nebula daemon listening port");
DEFINE_string(username, "", "Username used to authenticate");
DEFINE_string(password, "", "Password used to authenticate");


int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);

    using nebula::graph::CliManager;

    CliManager cli;
    if (!cli.connect(FLAGS_addr, FLAGS_port, FLAGS_username, FLAGS_password)) {
        exit(1);
    }

    cli.loop();
}


