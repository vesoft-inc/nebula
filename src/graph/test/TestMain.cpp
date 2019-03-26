/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "graph/test/TestEnv.h"
#include "fs/TempDir.h"
#include "meta/test/TestUtils.h"

DECLARE_string(meta_server_addrs);

using nebula::graph::TestEnv;
using nebula::graph::gEnv;
DECLARE_string(meta_server_addrs);
using nebula::meta::TestUtils;
using nebula::fs::TempDir;

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    FLAGS_meta_server_addrs = folly::stringPrintf("127.0.0.1:44503");

    gEnv = new TestEnv();   // gtest will delete this env object for us
    ::testing::AddGlobalTestEnvironment(gEnv);

    int32_t localMetaPort = 10001;
    FLAGS_meta_server_addrs = folly::stringPrintf("127.0.0.1:%d", localMetaPort);

    // need to start meta server
    TempDir rootPath("/tmp/MetaClientTest.XXXXXX");
    auto metaServer = TestUtils::mockServer(localMetaPort, rootPath.path());

    return RUN_ALL_TESTS();
}
