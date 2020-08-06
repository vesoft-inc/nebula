/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestBase.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TraverseTestBase.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class TTLTest : public TestBase {
protected:
    // Field Type Null Key Default Extra
    using SchemaTuple =
        std::tuple<std::string, std::string, bool, std::string, std::string, std::string>;

    void SetUp() override {
        TestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        TestBase::TearDown();
    }
};

TEST_F(TTLTest, schematest) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW HOSTS";
        client->execute(query, resp);
        std::vector<
            std::tuple<std::string, std::string, std::string, int, std::string, std::string>>
            expected{
                {"127.0.0.1",
                 std::to_string(gEnv->storageServerPort()),
                 "online",
                 0,
                 "No valid partition",
                 "No valid partition"},
                {"Total", "", "", 0, "", ""},
            };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }

    // Tag with TTL test
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG person(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<SchemaTuple> expected{
            {"name", "string", false, "", "", ""},
            {"email", "string", false, "", "", ""},
            {"age", "int", false, "", "", ""},
            {"gender", "string", false, "", "", ""},
            {"row_timestamp", "timestamp", false, "", "", ""},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG `person` (\n"
                               "  `name` string,\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string,\n"
                               "  `row_timestamp` timestamp\n"
                               ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"person", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG man(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_duration = 100, ttl_col = \"row_timestamp\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG man";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG `man` (\n"
                               "  `name` string,\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string,\n"
                               "  `row_timestamp` timestamp\n"
                               ") ttl_duration = 100, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"man", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // Disable implicit ttl mode
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG woman(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_duration = 100";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Disable when ttl_col is not an integer column or a timestamp column
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG woman(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_col = \"name\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Ttl_duration less than 0
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG woman(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_duration = 0, ttl_col = \"row_timestamp\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG woman";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG `woman` (\n"
                               "  `name` string,\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string,\n"
                               "  `row_timestamp` timestamp\n"
                               ") ttl_duration = 0, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"woman", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Only ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG only_ttl_col(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_col = \"row_timestamp\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG only_ttl_col";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG `only_ttl_col` (\n"
                               "  `name` string,\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string,\n"
                               "  `row_timestamp` timestamp\n"
                               ") ttl_duration = 0, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"only_ttl_col", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "ttl_duration = 50, ttl_col = \"row_timestamp\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG woman";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG `woman` (\n"
                               "  `name` string,\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string,\n"
                               "  `row_timestamp` timestamp\n"
                               ") ttl_duration = 50, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"woman", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Failed when alter tag to set ttl_col on not integer and timestamp column
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "ttl_col = \"name\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "Drop (name) ttl_duration = 200";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG woman";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG `woman` (\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string,\n"
                               "  `row_timestamp` timestamp\n"
                               ") ttl_duration = 200, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"woman", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // When the column is as TTL column, droping column
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "Drop (row_timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG woman";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::string createTagStr = "CREATE TAG `woman` (\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string\n"
                               ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"woman", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // ADD ttl property
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "ttl_duration = 100, ttl_col = \"age\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG woman";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG `woman` (\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string\n"
                               ") ttl_duration = 100, ttl_col = age";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"woman", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Change ttl col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "CHANGE (age string)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop ttl property
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman ttl_col = \"\" ";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG woman";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG `woman` (\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string\n"
                               ") ttl_duration = 0, ttl_col = \"\"";;
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"woman", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Change succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "CHANGE (age string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG woman";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG `woman` (\n"
                               "  `email` string,\n"
                               "  `age` string,\n"
                               "  `gender` string\n"
                               ") ttl_duration = 0, ttl_col = \"\"";;
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"woman", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // Edge with TTL test
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE work(number string, start_time timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE work";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<SchemaTuple> expected{
            {"number", "string", false, "", "", ""},
            {"start_time", "timestamp", false, "", "", ""},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE `work` (\n"
                               "  `number` string,\n"
                               "  `start_time` timestamp\n"
                               ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE work1(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_duration = 100, ttl_col = \"row_timestamp\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE `work1` (\n"
                               "  `name` string,\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string,\n"
                               "  `row_timestamp` timestamp\n"
                               ") ttl_duration = 100, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work1", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // Disable implicit ttl mode
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE work2(number string, start_time timestamp)"
                            "ttl_duration = 100";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Disable when ttl_col is not an integer column or a timestamp column
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE work2(number string, start_time timestamp)"
                            "ttl_col = \"name\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Ttl duration less than 0
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE work2(name string, email string, "
                            "age int, gender string, start_time timestamp)"
                            "ttl_duration = 0, ttl_col = \"start_time\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE `work2` (\n"
                               "  `name` string,\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string,\n"
                               "  `start_time` timestamp\n"
                               ") ttl_duration = 0, ttl_col = start_time";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Only ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE edge_only_ttl_col(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_col = \"row_timestamp\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE edge_only_ttl_col";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE `edge_only_ttl_col` (\n"
                               "  `name` string,\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string,\n"
                               "  `row_timestamp` timestamp\n"
                               ") ttl_duration = 0, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"edge_only_ttl_col", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "ttl_duration = 50, ttl_col = \"start_time\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;

        std::string query = "SHOW CREATE EDGE work2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE `work2` (\n"
                               "  `name` string,\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string,\n"
                               "  `start_time` timestamp\n"
                               ") ttl_duration = 50, ttl_col = start_time";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Failed when alter edge to set ttl_col on not integer and timestamp column
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "ttl_col = \"name\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "Drop (name) ttl_duration = 200";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE `work2` (\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string,\n"
                               "  `start_time` timestamp\n"
                               ") ttl_duration = 200, ttl_col = start_time";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // When the column is as TTL column, droping column
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "Drop (start_time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE `work2` (\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string\n"
                               ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Add ttl property
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER Edge work2 "
                            "ttl_duration = 100, ttl_col = \"age\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE EDGE `work2` (\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string\n"
                               ") ttl_duration = 100, ttl_col = age";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Change ttl col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "CHANGE (age string)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop ttl property
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "ttl_col = \"\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE EDGE `work2` (\n"
                               "  `email` string,\n"
                               "  `age` int,\n"
                               "  `gender` string\n"
                               ") ttl_duration = 0, ttl_col = \"\"";;
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Change succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "CHANGE (age string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE EDGE `work2` (\n"
                               "  `email` string,\n"
                               "  `age` string,\n"
                               "  `gender` string\n"
                               ") ttl_duration = 0, ttl_col = \"\"";;
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(TTLTest, Datatest) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "CREATE SPACE default_space(partition_num=9, "
                           "replica_factor=1, charset=utf8, collate=utf8_bin)";
        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string cmd2 = "USE default_space";
        auto code2 = client->execute(cmd2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        cpp2::ExecutionResponse resp3;
        std::string cmd3 = "CREATE TAG person(id int) "
                           "ttl_col=\"id\", ttl_duration=100";
        auto code3 = client->execute(cmd3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);

        cpp2::ExecutionResponse resp4;
        std::string cmd4 = "CREATE TAG career(id int)";
        auto code4 = client->execute(cmd4, resp4);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code4);

        cpp2::ExecutionResponse resp5;
        std::string cmd5 = "CREATE Edge like(id int) "
                           "ttl_col=\"id\", ttl_duration=100";
        auto code5 = client->execute(cmd5, resp5);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code5);

        cpp2::ExecutionResponse resp6;
        std::string cmd6 = "CREATE Edge friend(id int)";
        auto code6 = client->execute(cmd6, resp6);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code6);

        sleep(FLAGS_heartbeat_interval_secs + 3);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX person(id) "
                          "VALUES 1:(100), 2:(200)";

        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT VERTEX career(id) "
                          "VALUES 2:(200)";

        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }

    // Fetch tag test
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON person 1";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t, int>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON person 1 YIELD person.id as id";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t, int>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON * 1";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t, int>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON person 2";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t, int>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON person 2 YIELD person.id as id";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t, int>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON career 2";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{{"VertexID"}, {"career.id"}};
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, int>> expected = {
            {2, 200},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON career 2 YIELD career.id";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{{"VertexID"}, {"career.id"}};
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, int>> expected = {
            {2, 200},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON * 2";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{{"VertexID"}, {"career.id"}};
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));
        std::vector<std::tuple<int64_t, int>> expected = {
            {2, 200},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }

    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE like(id) "
                          "VALUES 100->1:(100), "
                          "100->2:(200)";

        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cpp2::ExecutionResponse resp1;
        std::string cmd1 = "INSERT EDGE friend(id) "
                           "VALUES 100->1:(100), "
                           "100->2:(200)";

        auto code1 = client->execute(cmd1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);
    }
    // Fetch edge test
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON friend 100->1,100->2";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"friend._src"},
            {"friend._dst"},
            {"friend._rank"},
            {"friend.id"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int>> expected = {
            {100, 1, 0, 100},
            {100, 2, 0, 200},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON friend 100->1,100->2 YIELD friend.id AS id";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"friend._src"},
            {"friend._dst"},
            {"friend._rank"},
            {"id"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t, int64_t, int64_t, int>> expected = {
            {100, 1, 0, 100},
            {100, 2, 0, 200},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON like 100->1,100->2";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t, int64_t, int64_t, int>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "FETCH PROP ON like 100->1,100->2 YIELD like.id AS id";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t, int64_t, int64_t, int>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }

    // GO test
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER friend";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"friend._dst"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {1},
            {2},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER friend WHERE friend.id == 100";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"friend._dst"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {1},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER friend YIELD friend.id";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"friend.id"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int>> expected = {
            {100},
            {200},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 1 OVER friend reversely";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"friend._dst"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {100},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 1 OVER friend reversely YIELD friend.id";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"friend.id"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int>> expected = {
            {100},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER friend bidirect";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"friend._dst"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {1},
            {2},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER friend bidirect YIELD friend.id";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"friend.id"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int>> expected = {
            {100},
            {200},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }

    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like WHERE like.id == 100;";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like YIELD like.id";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like YIELD like.id AS id, "
                          "$$.person.id AS pid";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int, int>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 1 OVER like reversely";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 1 OVER like reversely YIELD like.id";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like bidirect";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int64_t>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like bidirect YIELD like.id";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::tuple<int>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "INSERT EDGE like(id) "
                          "VALUES 100->3:(2436781720)";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"like._dst"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {3},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like WHERE like.id == 2436781720";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"like._dst"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {3},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GO FROM 100 OVER like WHERE like._dst == 3";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<std::string> expectedColNames{
            {"like._dst"},
        };
        ASSERT_TRUE(verifyColNames(resp, expectedColNames));

        std::vector<std::tuple<int64_t>> expected = {
            {3},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DROP SPACE default_space";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

}   // namespace graph
}   // namespace nebula
