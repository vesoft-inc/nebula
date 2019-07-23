/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "base/Base.h"
#include <gtest/gtest.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Baton.h>
#include "meta/processors/admin/Balancer.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"
#include "fs/TempDir.h"

namespace nebula {
namespace meta {

TEST(BalanceIntegrationTest, SimpleTest) {
    auto sc = std::make_unique<testing::ServerContext>();
    auto handler = std::make_shared<nebula::storage::StorageServiceHandler>(nullptr, nullptr);
    sc->mockCommon("storage", 0, handler);
    LOG(INFO) << "Start storage server on " << sc->port_;
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


