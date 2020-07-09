/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"
#include "mock/test/TestEnv.h"
#include "mock/test/TestBase.h"
#include <gtest/gtest.h>

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class SnapshotTest : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
    }

    void TearDown() override {
        TestBase::TearDown();
    }

    static void SetUpTestCase() {
        client_ = gEnv->getGraphClient();
        ASSERT_NE(nullptr, client_);
    }

    static void TearDownTestCase() {
        client_.reset();
    }

protected:
    static std::unique_ptr<GraphClient>     client_;
};

std::unique_ptr<GraphClient> SnapshotTest::client_{nullptr};

TEST_F(SnapshotTest, TestAll) {
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE mySpace(partition_num=1, replica_factor=1)";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SNAPSHOT";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    std::string snapshotName;
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "SHOW SNAPSHOTS";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(resp.__isset.data);
        auto snapshots = resp.get_data();
        std::vector<std::string> colNames = {"Name", "Status", "Hosts"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        ASSERT_EQ(1, snapshots->rows.size());
        snapshotName.append(snapshots->rows[0].values[0].getStr());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SNAPSHOT " + snapshotName;
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "SHOW SNAPSHOTS";
        auto code = client_->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(resp.__isset.data);
        auto snapshots = resp.get_data();
        std::vector<std::string> colNames = {"Name", "Status", "Hosts"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        ASSERT_TRUE(snapshots->rows.empty());
    }
}

}   // namespace graph
}   // namespace nebula
