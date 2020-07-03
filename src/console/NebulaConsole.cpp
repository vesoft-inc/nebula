/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "console/CliManager.h"
#include "fs/FileUtils.h"

DEFINE_string(addr, "127.0.0.1", "Nebula daemon IP address");
DEFINE_int32(port, 0, "Nebula daemon listening port");
DEFINE_string(u, "root", "Username used to authenticate");
DEFINE_string(p, "", "Password used to authenticate");


int main(int argc, char *argv[]) {
    google::SetVersionString(nebula::versionString());
    folly::init(&argc, &argv, true);
    if (argc > 1/*executable name*/) {
        std::cout << "Unrecognized arguments: ";
        for (int i = 1; i < argc; ++i) {
            std::cout << argv[i] << " ";
        }
        std::cout << std::endl;
        return EXIT_FAILURE;
    }

    using nebula::graph::CliManager;
    using nebula::fs::FileUtils;
    if (FLAGS_port == 0) {
        // If port not provided, we use the one in etc/nebula-graphd.conf
        auto path = FileUtils::readLink("/proc/self/exe").value();
        auto dir = FileUtils::dirname(path.c_str());
        static const std::regex pattern("--port=([0-9]+)");
        FileUtils::FileLineIterator iter(dir + "/../etc/nebula-graphd.conf", &pattern);
        if (iter.valid()) {
            auto &smatch = iter.matched();
            FLAGS_port = folly::to<int>(smatch[1].str());
        }
    }
    if (FLAGS_port == 0) {
        fprintf(stderr, "--port must be specified\n");
        return EXIT_FAILURE;
    }

    CliManager cli;
    if (!cli.connect()) {
        return EXIT_FAILURE;
    }

    cli.loop();
    return EXIT_SUCCESS;
}


