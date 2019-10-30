/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(load_data_interval_secs);

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
    sleep(FLAGS_load_data_interval_secs + 1);

    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SNAPSHOT";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
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

    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SNAPSHOT " + sname;
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

}   // namespace graph
}   // namespace nebula
