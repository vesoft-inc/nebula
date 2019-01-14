/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "graph/test/TestEnv.h"

using nebula::graph::TestEnv;
using nebula::graph::gEnv;

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);

    gEnv = new TestEnv();   // gtest will delete this env object for us
    ::testing::AddGlobalTestEnvironment(gEnv);

    return RUN_ALL_TESTS();
}
