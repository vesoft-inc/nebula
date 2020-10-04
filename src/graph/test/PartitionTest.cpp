/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"

DECLARE_int32(max_parts_num);
namespace nebula {
namespace graph {
class PartitionTest : public TestBase {
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

TEST_F(PartitionTest, limit_partitions) {
    auto client = gEnv->getClient();
    FLAGS_max_parts_num = 100;
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space1 (partition_num=50)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "CREATE SPACE space2 (partition_num=51)";
        code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "drop SPACE space1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_partitions(partition_num=101)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
}
}   // namespace graph
}   // namespace nebula
