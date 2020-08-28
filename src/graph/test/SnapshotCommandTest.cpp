/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class SnapshotCommandTest : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        TestBase::TearDown();
    }
};

TEST_F(SnapshotCommandTest, TestSnapshot) {
    auto client = gEnv->getClient();

    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE mySpace(partition_num=1, replica_factor=1)";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);

    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SNAPSHOT";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    std::string sname;
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "SHOW SNAPSHOTS";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        const auto& snapshots = resp.get_rows();
        ASSERT_EQ(1, snapshots->size());
        sname.append(snapshots[0].data()->get_columns().data()->get_str());
    }

    {
        std::vector<std::string> checkpoints;
        checkpoints.emplace_back(folly::stringPrintf("%s/disk1/nebula/1/checkpoints/%s/data",
                                                     gEnv->getStorageRootPath().data(),
                                                     sname.data()));
        checkpoints.emplace_back(folly::stringPrintf("%s/disk2/nebula/1/checkpoints/%s/data",
                                                     gEnv->getStorageRootPath().data(),
                                                     sname.data()));
        checkpoints.emplace_back(folly::stringPrintf("%s/disk1/nebula/0/checkpoints/%s/data",
                                                     gEnv->getMetaRootPath().data(),
                                                     sname.data()));
        for (auto& cp : checkpoints) {
            ASSERT_TRUE(fs::FileUtils::exist(cp));
            auto files = fs::FileUtils::listAllFilesInDir(cp.data());
            ASSERT_LE(3, files.size());
        }
    }

    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SNAPSHOT " + sname;
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        std::vector<std::string> checkpoints;
        checkpoints.emplace_back(folly::stringPrintf("%s/disk1/nebula/1/checkpoints/%s",
                                                     gEnv->getStorageRootPath().data(),
                                                     sname.data()));
        checkpoints.emplace_back(folly::stringPrintf("%s/disk2/nebula/1/checkpoints/%s",
                                                     gEnv->getStorageRootPath().data(),
                                                     sname.data()));
        checkpoints.emplace_back(folly::stringPrintf("%s/disk1/nebula/0/checkpoints/%s",
                                                     gEnv->getMetaRootPath().data(),
                                                     sname.data()));
        for (auto& cp : checkpoints) {
            ASSERT_FALSE(fs::FileUtils::exist(cp));
        }
    }
}

}   // namespace graph
}   // namespace nebula
