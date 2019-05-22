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
    {
        // Test space not exist
        cpp2::ExecutionResponse resp;
        std::string query = "USE not_exist_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG man(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_load_data_interval_secs + 1);
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG account(id int, balance double)";
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
    {
        // Test the tag has been existed.
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG account(id int, balance double)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // Test exist
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG account(id int, balance double)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG account "
                            "ADD (col1 int, col2 string), "
                            "CHANGE (balance string), "
                            "DROP (id)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG account DROP (id)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG account";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
                {"col1", "int"},
                {"col2", "string"},
                {"balance", "string"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Test tag not exist
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG not_exist";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE buy(id int, time string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // Test the edge has been existed.
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
    {
        // Test edge not exist
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE not_exist";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW EDGES";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 1>> expected{
            {"buy"},
            {"education"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
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
    // Test different tag and edge in different space
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
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

}   // namespace graph
}   // namespace nebula
