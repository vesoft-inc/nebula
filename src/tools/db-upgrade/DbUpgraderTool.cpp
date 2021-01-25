/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "utils/NebulaKeyUtils.h"
#include "tools/db-upgrade/NebulaKeyUtilsV1.h"

int main(int argc, char *argv[]) {
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return 0;
}
