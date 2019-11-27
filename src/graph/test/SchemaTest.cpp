/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"

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
        ASSERT_EQ(cpp2::ErrorCode::E_STATEMENT_EMTPY, code);
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
        std::string query = "SHOW HOSTS";
        client->execute(query, resp);
        std::vector<std::tuple<std::string, std::string, std::string,
                               int, std::string, std::string>> expected {
            {"127.0.0.1", std::to_string(gEnv->storageServerPort()), "online", 0,
             "No valid partition", "No valid partition"},
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
        std::string query = "SHOW CREATE SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createSpaceStr = "CREATE SPACE default_space ("
                                     "partition_num = 9, "
                                     "replica_factor = 1)";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"default_space", createSpaceStr},
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
            {2, "space_with_default_options", 100, 1},
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
    // show parts of default_space
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW PARTS; # before leader election";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(9, (*(resp.get_rows())).size());
        std::string host = "127.0.0.1:" + std::to_string(gEnv->storageServerPort());
        std::vector<std::tuple<int, std::string, std::string, std::string>> expected;
        for (int32_t partId = 1; partId <= 9; partId++) {
            expected.emplace_back(std::make_tuple(partId, "", host, ""));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test same prop name
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG samePropTag(name string, name int)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Test same prop name
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE samePropEdge(name string, name int)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
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
    // Create Tag with default value
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG person_with_default(name string, age int default 18)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG person_type_mismatch"
                            "(name string, age int default \"hello\")";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG person (\n"
                               "  name string,\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  row_timestamp timestamp\n"
                               ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"person", createTagStr},
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
        std::string query = "CREATE TAG person(id int)";
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG person (\n"
                               "  name string,\n"
                               "  email string,\n"
                               "  age string,\n"
                               "  row_timestamp timestamp,\n"
                               "  col1 int,\n"
                               "  col2 string\n"
                               ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"person", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW TAGS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int32_t, std::string>> expected{
            {3, "tag1"},
            {4, "person"},
            {5, "person_with_default"},
            {6, "upper"},
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE buy_with_default(id int, time string default \"\")";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE buy_type_mismatch(id int, time string default 0)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
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
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
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
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"id", "int"},
            {"time", "string"},
        };
        EXPECT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE buy";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE buy (\n"
                                   "  id int,\n"
                                   "  time string\n"
                                   ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"buy", createEdgeStr},
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
        std::vector<std::tuple<int32_t, std::string>> expected{
            {7,  "edge1"},
            {8,  "buy"},
            {9,  "buy_with_default"},
            {10, "education"},
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
                {"school", "int"},
                {"col1", "int"},
                {"col2", "string"},
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE education";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE education (\n"
                                   "  school int,\n"
                                   "  col1 int,\n"
                                   "  col2 string\n"
                                   ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"education", createEdgeStr},
        };
        EXPECT_TRUE(verifyResult(resp, expected));
    }
    // show parts of default_space
    {
        auto kvstore = gEnv->storageServer()->kvStore_.get();
        GraphSpaceID spaceId = 1;  // default_space id is 1
        nebula::storage::TestUtils::waitUntilAllElected(kvstore, spaceId, 9);

        cpp2::ExecutionResponse resp;
        std::string query = "SHOW PARTS; # after leader election";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(9, (*(resp.get_rows())).size());
        std::string host = "127.0.0.1:" + std::to_string(gEnv->storageServerPort());
        std::vector<std::tuple<int, std::string, std::string, std::string>> expected;
        for (int32_t partId = 1; partId <= 9; partId++) {
            expected.emplace_back(std::make_tuple(partId, host, host, ""));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
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
        std::vector<std::tuple<int32_t, std::string>> expected{
            {1012, "animal"},
            {1013, "person"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test multi sentence
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE test_multi";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "USE test_multi; CREATE Tag test_tag(); SHOW TAGS;";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int32_t, std::string>> expected1{
            {1015, "test_tag"},
        };
        ASSERT_TRUE(verifyResult(resp, expected1));

        query = "USE test_multi; CREATE TAG test_tag1(); USE my_space; SHOW TAGS;";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int32_t, std::string>> expected2{
            {1012, "animal"},
            {1013, "person"},
        };
        ASSERT_TRUE(verifyResult(resp, expected2));

        query = "DROP SPACE test_multi";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
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
        std::string query = "SHOW HOSTS";
        client->execute(query, resp);
        ASSERT_EQ(1, (*(resp.get_rows())).size());
    }

    sleep(FLAGS_load_data_interval_secs + 1);
    int retry = 60;
    while (retry-- > 0) {
        auto spaceResult = gEnv->metaClient()->getSpaceIdByNameFromCache("default_space");
        if (!spaceResult.ok()) {
            return;
        }
        sleep(1);
    }
    LOG(FATAL) << "Space still exists after sleep " << retry << " seconds";
}


TEST_F(SchemaTest, TTLtest) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW HOSTS";
        client->execute(query, resp);
        std::vector<std::tuple<std::string, std::string, std::string,
                               int, std::string, std::string>> expected {
            {"127.0.0.1", std::to_string(gEnv->storageServerPort()), "online", 0,
             "No valid partition", "No valid partition"},
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG person (\n"
                               "  name string,\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  row_timestamp timestamp\n"
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
                            "ttl_duration = 100, ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG man";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG man (\n"
                               "  name string,\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  row_timestamp timestamp\n"
                               ") ttl_duration = 100, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"man", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // Abnormal test
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG woman";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG woman (\n"
                               "  name string,\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  row_timestamp timestamp\n"
                               ") ttl_duration = 0, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"woman", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG only_ttl_col(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG only_ttl_col";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG only_ttl_col (\n"
                               "  name string,\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  row_timestamp timestamp\n"
                               ") ttl_duration = 0, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"only_ttl_col", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "ttl_duration = 50, ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG woman";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagStr = "CREATE TAG woman (\n"
                               "  name string,\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  row_timestamp timestamp\n"
                               ") ttl_duration = 50, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"woman", createTagStr},
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
        std::string createTagStr = "CREATE TAG woman (\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  row_timestamp timestamp\n"
                               ") ttl_duration = 200, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"woman", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // When the column is as TTL column, droping column failed
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "Drop (row_timestamp)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // First remove TTL property, then drop column
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG woman "
                            "ttl_col = age";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG woman";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::string createTagStr = "CREATE TAG woman (\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  row_timestamp timestamp\n"
                               ") ttl_duration = 200, ttl_col = age";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"woman", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
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
        std::string createTagStr = "CREATE TAG woman (\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string\n"
                               ") ttl_duration = 200, ttl_col = age";
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
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"number", "string"},
            {"start_time", "timestamp"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE work (\n"
                               "  number string,\n"
                               "  start_time timestamp\n"
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
                            "ttl_duration = 100, ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE work1 (\n"
                               "  name string,\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  row_timestamp timestamp\n"
                               ") ttl_duration = 100, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work1", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }

    // Abnormal test
    {
        // Disable implicit ttl mode
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE work2(number string, start_time timestamp)"
                            "ttl_duration = 100";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // Disable when ttl_col is not an integer column or a timestamp column
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE work2(number string, start_time timestamp)"
                            "ttl_col = name";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE work2(name string, email string, "
                            "age int, gender string, start_time timestamp)"
                            "ttl_duration = -100, ttl_col = start_time";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE work2 (\n"
                               "  name string,\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  start_time timestamp\n"
                               ") ttl_duration = 0, ttl_col = start_time";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE edge_only_ttl_col(name string, email string, "
                            "age int, gender string, row_timestamp timestamp)"
                            "ttl_col = row_timestamp";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE edge_only_ttl_col";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE edge_only_ttl_col (\n"
                               "  name string,\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  row_timestamp timestamp\n"
                               ") ttl_duration = 0, ttl_col = row_timestamp";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"edge_only_ttl_col", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "ttl_duration = 50, ttl_col = start_time";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;

        std::string query = "SHOW CREATE EDGE work2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE work2 (\n"
                               "  name string,\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  start_time timestamp\n"
                               ") ttl_duration = 50, ttl_col = start_time";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // Failed when alter edge to set ttl_col on not integer and timestamp column
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "ttl_col = name";
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
        std::string createEdgeStr = "CREATE EDGE work2 (\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  start_time timestamp\n"
                               ") ttl_duration = 200, ttl_col = start_time";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // When the column is as TTL column, droping column failed
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "Drop (start_time)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // First remove TTL property, then drop column
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "ttl_col = age";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE work2 (\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string,\n"
                               "  start_time timestamp\n"
                               ") ttl_duration = 200, ttl_col = age";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createEdgeStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE work2 "
                            "Drop (start_time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE work2";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"email", "string"},
            {"age", "int"},
            {"gender", "string"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE work2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE work2 (\n"
                               "  email string,\n"
                               "  age int,\n"
                               "  gender string\n"
                               ") ttl_duration = 200, ttl_col = age";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"work2", createEdgeStr},
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

}   // namespace graph
}   // namespace nebula
