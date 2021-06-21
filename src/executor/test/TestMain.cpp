/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include <gflags/gflags_declare.h>

#include "common/base/Base.h"

DECLARE_bool(enable_lifetime_optimize);

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    // This need the analysis in scheduler so disable it when only test executor itself.
    FLAGS_enable_lifetime_optimize = false;

    return RUN_ALL_TESTS();
}
