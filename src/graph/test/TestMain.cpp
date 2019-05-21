/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "graph/test/TestEnv.h"
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"

using nebula::graph::TestEnv;
using nebula::graph::gEnv;
using nebula::meta::TestUtils;
using nebula::fs::TempDir;

DECLARE_string(meta_server_addrs);

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    gEnv = new TestEnv();   // gtest will delete this env object for us
    ::testing::AddGlobalTestEnvironment(gEnv);

    // Let the system choose an available port for us
    int32_t localMetaPort = 0;
    // need to start meta server
    TempDir rootPath("/tmp/MetaClientTest.XXXXXX");
    auto metaServer = TestUtils::mockServer(localMetaPort, rootPath.path());

    FLAGS_meta_server_addrs = folly::stringPrintf("127.0.0.1:%d", metaServer->port_);
    return RUN_ALL_TESTS();
}
