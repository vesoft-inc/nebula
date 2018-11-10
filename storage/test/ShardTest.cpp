/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include <gtest/gtest.h>
#include "fs/TempDir.h"
#include "storage/Shard.h"

namespace vesoft {
namespace vgraph {
namespace storage {

TEST(ShardTest, SimpleTest) {
    LOG(INFO) << "Simple test for shard class...";
    Shard shard("default", 1, "", "");
    EXPECT_EQ(ResultCode::SUCCESSED, shard.put("k1", "v1"));

    std::string value;
    EXPECT_EQ(ResultCode::SUCCESSED, shard.get("k1", value));
}

}  // namespace storage
}  // namespace vgraph
}  // namespace vesoft


int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}


