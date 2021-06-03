/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include <gtest/gtest.h>
#include "storage/admin/ListClusterInfoProcessor.h"
#include "mock/MockCluster.h"
#include "mock/MockData.h"

namespace nebula {
namespace storage {
TEST(ListClusterInfoTest, simpleTest) {
    fs::TempDir dataPath("/tmp/ListClusterInfo_Test_src.XXXXXX");
    mock::MockCluster cluster;
    cluster.initStorageKV(dataPath.path());
    auto* env = cluster.storageEnv_.get();

    // Begin list clusterinfo
    {
        auto* processor = ListClusterInfoProcessor::instance(env);
        cpp2::ListClusterInfoReq req;
        auto fut = processor->getFuture();
        processor->process(req);
        auto resp = std::move(fut).get();
        EXPECT_EQ(0, resp.result.failed_parts.size());
        auto data_root = env->kvstore_->getDataRoot();
        auto dir = resp.get_dir();
        ASSERT_EQ(dir.get_data().size(), data_root.size());
        int i = 0;
        for (auto d : dir.get_data()) {
            ASSERT_EQ(d, data_root[i]);
            i++;
        }
        std::cout << dir.get_root() << std::endl;
        ASSERT_FALSE(dir.get_data().empty());
    }
}

}  // namespace storage
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
