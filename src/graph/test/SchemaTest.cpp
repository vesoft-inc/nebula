/* Copyright (c) 2018 vesoft inc. All rights reserved.
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

class SchemaTest : public TestBase {
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

TEST_F(SchemaTest, TestComment) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    // Test command is comment
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "# CREATE TAG TAG1";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_DO_NONE, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "SHOW SPACES # show all spaces";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(SchemaTest, metaCommunication) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ADD HOSTS 127.0.0.1:1000, 127.0.0.1:1100";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        meta::TestUtils::registerHB(
                network::NetworkUtils::toHosts("127.0.0.1:1000, 127.0.0.1:1100").value());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW HOSTS";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 3>> expected{
            {"127.0.0.1", "1000", "offline"},
            {"127.0.0.1", "1100", "offline"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test space not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE not_exist_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Test create space succeeded
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE default_space(partition_num=9, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int, std::string, int, int>> expected{
            {1, "default_space", 9, 1},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test desc space command
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int, std::string, int, int>> expected{
            {1, "default_space", 9, 1},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_with_default_options";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE SPACE space_with_default_options";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int, std::string, int, int>> expected{
            {2, "space_with_default_options", 1024, 1},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE space_with_default_options";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Test create tag without prop
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG tag1()";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG tag1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 1>> expected{};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG tag1 ADD (id int, name string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG tag1";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
                {"id", "int"},
                {"name", "string"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test create tag succeeded
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
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"name", "string"},
            {"email", "string"},
            {"age", "int"},
            {"gender", "string"},
            {"row_timestamp", "timestamp"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test desc tag command
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"name", "string"},
            {"email", "string"},
            {"age", "int"},
            {"gender", "string"},
            {"row_timestamp", "timestamp"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test unreserved keyword
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG upper(name string, EMAIL string, "
                            "age int, gender string, row_timestamp timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG upper";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"name", "string"},
            {"email", "string"},
            {"age", "int"},
            {"gender", "string"},
            {"row_timestamp", "timestamp"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test existent tag
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG person(id int, balance double)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Test nonexistent tag
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG not_exist";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Test alter tag
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG person "
                            "ADD (col1 int, col2 string), "
                            "CHANGE (age string), "
                            "DROP (gender)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG person DROP (gender)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG person";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
                {"name", "string"},
                {"email", "string"},
                {"age", "string"},
                {"row_timestamp", "timestamp"},
                {"col1", "int"},
                {"col2", "string"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test create edge without prop
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE edge1()";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE edge1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE edge1 ADD (id int, name string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE edge1";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
                {"id", "int"},
                {"name", "string"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test create edge succeeded
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE buy(id int, time string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Test existent edge
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE buy(id int, time string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE buy";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"id", "int"},
            {"time", "string"},
        };
        EXPECT_TRUE(verifyResult(resp, expected));
    }
    // Test nonexistent edge
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE not_exist";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Test desc edge
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC EDGE buy";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"id", "int"},
            {"time", "string"},
        };
        EXPECT_TRUE(verifyResult(resp, expected));
    }
    // Test edge not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE not_exist";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Test create edge succeeded
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE education(id int, time timestamp, school string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE education";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"id", "int"},
            {"time", "timestamp"},
            {"school", "string"},
        };
        EXPECT_TRUE(verifyResult(resp, expected));
    }
    // Test show edges
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW EDGES";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 1>> expected{
            {"edge1"},
            {"edge1"},
            {"buy"},
            {"education"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test alter edge
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE education "
                            "ADD (col1 int, col2 string), "
                            "CHANGE (school int), "
                            "DROP (id, time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE education DROP (id, time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE education";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
                {"col1", "int"},
                {"col2", "string"},
                {"school", "int"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test multi sentences
    {
        cpp2::ExecutionResponse resp;
        std::string query;
        for (auto i = 0u; i < 1000; i++) {
            query += "CREATE TAG tag10" + std::to_string(i) + "(name string);";
        }
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query;
        for (auto i = 0u; i < 1000; i++) {
            query = "DESCRIBE TAG tag10" + std::to_string(i);
            client->execute(query, resp);
            std::vector<uniform_tuple_t<std::string, 2>> expected{
                {"name", "string"},
            };
            ASSERT_TRUE(verifyResult(resp, expected));
        }
    }
    // Test drop tag
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Test drop edge
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE buy";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Test different tag and edge in different space
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE my_space(partition_num=9, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE my_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG animal(name string, kind string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG animal";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"name", "string"},
            {"kind", "string"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test the same tag in diff space
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG person(name string, interest string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW TAGS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 1>> expected{
            {"animal"},
            {"person"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test drop space
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE my_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW SPACES";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 1>> expected{
            {"default_space"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW SPACES";
        client->execute(query, resp);
        ASSERT_EQ(0, (*(resp.get_rows())).size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REMOVE HOSTS 127.0.0.1:1000, 127.0.0.1:1100";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW HOSTS";
        client->execute(query, resp);
        ASSERT_EQ(0, (*(resp.get_rows())).size());
    }
}


TEST_F(SchemaTest, TTLtest) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ADD HOSTS 127.0.0.1:1000, 127.0.0.1:1100";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        meta::TestUtils::registerHB(
                network::NetworkUtils::toHosts("127.0.0.1:1000, 127.0.0.1:1100").value());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW HOSTS";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 3>> expected{
            {"127.0.0.1", "1000", "offline"},
            {"127.0.0.1", "1100", "offline"},
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
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"name", "string"},
            {"email", "string"},
            {"age", "int"},
            {"gender", "string"},
            {"row_timestamp", "timestamp"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // TODO(YT) Add show create tag
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG man(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_duration = 100, ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG man";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"name", "string"},
            {"email", "string"},
            {"age", "int"},
            {"gender", "string"},
            {"row_timestamp", "timestamp"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Add show create tag test
    {
        // Disable implicit ttl mode
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG woman(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_duration = 100";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // Disable when ttl_col is not an integer column or a timestamp column
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG woman(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_col = name";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG woman(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_duration = -100, ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create tag instead
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG only_ttl_col(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT)  Use show create tag instead
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "ttl_duration = 50, ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Use show create tag instead
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG woman";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"name", "string"},
            {"email", "string"},
            {"age", "int"},
            {"gender", "string"},
            {"row_timestamp", "timestamp"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Failed when alter tag to set ttl_col on not integer and timestamp column
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "ttl_col = name";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Use show create tag instead
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG woman";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"name", "string"},
            {"email", "string"},
            {"age", "int"},
            {"gender", "string"},
            {"row_timestamp", "timestamp"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "Drop (name) ttl_duration = 200";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create tag
    {
        // When the column is as TTL column, droping column failed
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "Drop (row_timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create tag
    {
        // First remove TTL property, then drop column
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "ttl_col = age";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create tag
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "Drop (row_timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create tag

    // Edge ttl test
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE person(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"name", "string"},
            {"email", "string"},
            {"age", "int"},
            {"gender", "string"},
            {"row_timestamp", "timestamp"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // TODO(YT) Use show create edge test
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE man(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_duration = 100, ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create edge test
    {
        // Disable implicit ttl mode
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE woman(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_duration = 100";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // Disable when ttl_col is not an integer column or a timestamp column
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE woman(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_col = name";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE woman(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_duration = -100, ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create edge
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE only_ttl_col(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create edge
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE woman "
                            "ttl_duration = 50, ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create edge
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE woman "
                            "Drop (name) ttl_duration = 200";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create edge
    {
        // Failed when alter tag to set ttl_col on not integer and timestamp column
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE woman "
                            "ttl_col = email";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create edge
    {
        // When the column is as TTL column, droping column failed
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE woman "
                            "Drop (row_timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create edge
    {
        // First remove TTL property, then drop column
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE woman "
                            "ttl_col = age";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create edge
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE woman "
                            "drop (row_timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // TODO(YT) Use show create edge

    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REMOVE HOSTS 127.0.0.1:1000, 127.0.0.1:1100";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

}   // namespace graph
}   // namespace nebula
