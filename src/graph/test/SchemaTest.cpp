/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestBase.h"
#include "graph/test/TestEnv.h"
#include "meta/test/TestUtils.h"
#include "storage/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class SchemaTest : public TestBase {
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

class SchemaTestIssue1987 : public TestBase, public ::testing::WithParamInterface<std::string> {
protected:
    void SetUp() override {
        TestBase::SetUp();
        client_ = gEnv->getClient();
        ASSERT_NE(nullptr, client_);
        setupSchema();
    }

    void TearDown() override {
        {
            cpp2::ExecutionResponse resp;
            std::string cmd = "DROP SPACE issue1987";
            auto code = client_->execute(cmd, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        }
        TestBase::TearDown();
    }

    void setupSchema() {
        {
            cpp2::ExecutionResponse resp;
            std::string cmd = "CREATE SPACE issue1987";
            auto code = client_->execute(cmd, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        }
        {
            cpp2::ExecutionResponse resp;
            std::string cmd = "USE issue1987";
            auto code = client_->execute(cmd, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        }
    }

    std::unique_ptr<GraphClient> client_{nullptr};
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

TEST_F(SchemaTest, TestDefaultValue) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_for_default(partition_num=9, replica_factor=1);"
                            "USE space_for_default";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG default_tag(name string DEFAULT 10)";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG default_tag(name string, age int DEFAULT \"10\")";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG default_tag(name string  DEFAULT \"\", "
                          "age int DEFAULT \"10\")";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG default_tag(name string  DEFAULT 10, age int DEFAULT 10)";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Create tag with wrong default value of timestamp
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG default_school(name string, "
                          "create_time timestamp DEFAULT -1)";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);

        cmd = "CREATE TAG default_school(name string, create_time timestamp "
              "DEFAULT \"2900-10-10 10:00:00\")";
        code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE space_for_default";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(SchemaTest, TestSpace) {
    FLAGS_heartbeat_interval_secs = 1;
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    // Test space not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE not_exist_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Test space with default options
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
        std::vector<std::tuple<std::string, int, int, std::string, std::string>> expected{
            {"space_with_default_options", 100, 1, "utf8", "utf8_bin"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE space_with_default_options";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
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
        std::string query = "SHOW SPACES";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 1>> expected{
            {"default_space"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test desc space
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int, int, std::string, std::string>> expected{
            {"default_space", 9, 1, "utf8", "utf8_bin"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int, int, std::string, std::string>> expected{
            {"default_space", 9, 1, "utf8", "utf8_bin"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    // Show create space
    std::string createSpaceStr;
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        createSpaceStr = "CREATE SPACE `default_space` ("
                         "partition_num = 9, "
                         "replica_factor = 1, "
                         "charset = utf8, "
                         "collate = utf8_bin)";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"default_space", createSpaceStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Check resutl from show create
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        code = client->execute(createSpaceStr, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Test charset and collate
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_charset_collate (partition_num=9, "
                            "replica_factor=1, charset=utf8, collate=utf8_bin)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE SPACE space_charset_collate";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int, int, std::string, std::string>> expected{
            {"space_charset_collate", 9, 1, "utf8", "utf8_bin"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, false, {0}));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE space_charset_collate";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_charset (partition_num=9, "
                            "replica_factor=1, charset=utf8)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE SPACE space_charset";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int, int, std::string, std::string>> expected{
            {"space_charset", 9, 1, "utf8", "utf8_bin"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE space_charset";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_collate (partition_num=9, "
                            "replica_factor=1, collate=utf8_bin)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE SPACE space_collate";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int, int, std::string, std::string>> expected{
            {"space_collate", 9, 1, "utf8", "utf8_bin"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE space_collate";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_charset_collate_nomatch (partition_num=9, "
                            "replica_factor=1, charset = utf8, collate=gbk_bin)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_charset_collate_nomatch (partition_num=9, "
                            "replica_factor=1, charset = gbk, collate=utf8_bin)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_illegal_charset (partition_num=9, "
                            "replica_factor=1, charset = gbk)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_illegal_collate (partition_num=9, "
                            "replica_factor=1, collate = gbk_bin)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_capital (partition_num=9, "
                            "replica_factor=1, charset=UTF8, collate=UTF8_bin)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE SPACE space_capital";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int, int, std::string, std::string>> expected{
            {"space_capital", 9, 1, "utf8", "utf8_bin"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE space_capital";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }

    // Test IF NOT EXISTS And IF EXISTS
    {
        std::string query = "CREATE SPACE default_space";
        cpp2::ExecutionResponse resp;
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        std::string query = "CREATE SPACE IF NOT EXISTS default_space";
        cpp2::ExecutionResponse resp;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        std::string query = "DROP SPACE IF EXISTS not_exist_space";
        cpp2::ExecutionResponse resp;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        std::string query = "DROP SPACE not_exist_space";
        cpp2::ExecutionResponse resp;
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        std::string query = "CREATE SPACE exist_space";
        cpp2::ExecutionResponse resp;
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::string query1 = "DROP SPACE IF EXISTS exist_space";
        cpp2::ExecutionResponse resp1;
        auto code1 = client->execute(query1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);
    }
    // Test drop space
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
}

TEST_F(SchemaTest, TestTagAndEdge) {
    FLAGS_heartbeat_interval_secs = 1;
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
        std::string query = "CREATE SPACE default_space(partition_num=9, "
                            "replica_factor=1, charset=utf8, collate=utf8_bin)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, int, int, std::string, std::string>> expected{
            {"default_space", 9, 1, "utf8", "utf8_bin"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
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
    // Test create tag without prop
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG tag1()";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG IF NOT EXISTS tag1()";
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
        std::vector<SchemaTuple> expected{
            {"id", "int", false, "", "", ""},
            {"name", "string", false, "", "", ""},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test create tag succeeded
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG person(name string, email string DEFAULT \"NULL\", "
                            "age int, gender string, row_timestamp timestamp DEFAULT 2020)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Create Tag with duplicate field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG duplicate_tag(name string, name int)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG duplicate_tag(name string, name string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
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
        std::vector<SchemaTuple> expected{
            {"name", "string", false, "", "", ""},
            {"email", "string", false, "", "\"NULL\"", ""},
            {"age", "int", false, "", "", ""},
            {"gender", "string", false, "", "", ""},
            {"row_timestamp", "timestamp", false, "", "2020", ""},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test desc tag command
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<SchemaTuple> expected{
            {"name", "string", false, "", "", ""},
            {"email", "string", false, "", "\"NULL\"", ""},
            {"age", "int", false, "", "", ""},
            {"gender", "string", false, "", "", ""},
            {"row_timestamp", "timestamp", false, "", "2020", ""},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Show create tag
    std::string createTagStr;
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        createTagStr = "CREATE TAG `person` (\n"
                       "  `name` string,\n"
                       "  `email` string default \"NULL\",\n"
                       "  `age` int,\n"
                       "  `gender` string,\n"
                       "  `row_timestamp` timestamp default 2020\n"
                       ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"person", createTagStr},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Check resutl from show create
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        code = client->execute(createTagStr, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
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
        std::string query = "CREATE TAG upper(name string, ACCOUNT string, "
                            "age int, gender string, row_timestamp timestamp DEFAULT 100)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG upper";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::vector<SchemaTuple> expected{
            {"name", "string", false, "", "", ""},
            {"account", "string", false, "", "", ""},
            {"age", "int", false, "", "", ""},
            {"gender", "string", false, "", "", ""},
            {"row_timestamp", "timestamp", false, "", "100", ""},
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
        std::vector<SchemaTuple> expected{
            {"name", "string", false, "", "", ""},
            {"email", "string", false, "", "\"NULL\"", ""},
            {"age", "string", false, "", "", ""},
            {"row_timestamp", "timestamp", false, "", "2020", ""},
            {"col1", "int", false, "", "", ""},
            {"col2", "string", false, "", "", ""},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG person";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        createTagStr = "CREATE TAG `person` (\n"
                       "  `name` string,\n"
                       "  `email` string default \"NULL\",\n"
                       "  `age` string,\n"
                       "  `row_timestamp` timestamp default 2020,\n"
                       "  `col1` int,\n"
                       "  `col2` string\n"
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
        std::vector<std::tuple<std::string>> expected{
            {"tag1"},
            {"person"},
            {"person_with_default"},
            {"upper"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
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
        std::string query = "CREATE EDGE IF NOT EXISTS edge1()";
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
        std::vector<SchemaTuple> expected{
            {"id", "int", false, "", "", ""},
            {"name", "string", false, "", "", ""},
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
    // Create Edge with duplicate field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE duplicate_buy(time int, time string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE duplicate_buy(time int, time int)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE buy_with_default(id int, name string DEFAULT \"NULL\","
                            "time timestamp DEFAULT 2020)";
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
        std::vector<SchemaTuple> expected{
            {"id", "int", false, "", "", ""},
            {"time", "string", false, "", "", ""},
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
        std::vector<SchemaTuple> expected{
            {"id", "int", false, "", "", ""},
            {"time", "string", false, "", "", ""},
        };
        EXPECT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE buy_with_default";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createEdgeStr = "CREATE EDGE `buy_with_default` (\n"
                                   "  `id` int,\n"
                                   "  `name` string default \"NULL\",\n"
                                   "  `time` timestamp default 2020\n"
                                   ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"buy_with_default", createEdgeStr},
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
        std::vector<SchemaTuple> expected{
            {"id", "int", false, "", "", ""},
            {"time", "timestamp", false, "", "", ""},
            {"school", "string", false, "", "", ""},
        };
        EXPECT_TRUE(verifyResult(resp, expected));
    }
    // Test show edges
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW EDGES";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected{
            {"edge1"},
            {"buy"},
            {"buy_with_default"},
            {"education"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
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
        std::vector<SchemaTuple> expected{
            {"school", "int", false, "", "", ""},
            {"col1", "int", false, "", "", ""},
            {"col2", "string", false, "", "", ""},
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
            std::vector<SchemaTuple> expected{
                {"name", "string", false, "", "", ""},
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG not_exist_tag";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG IF EXISTS not_exist_tag";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG exist_tag(id int)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cpp2::ExecutionResponse resp1;
        std::string query1 = "DROP TAG IF EXISTS exist_tag";
        auto code1 = client->execute(query1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);
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
        std::string query = "DROP EDGE not_exist_edge";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE IF EXISTS not_exist_edge";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE exist_edge(id int)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        cpp2::ExecutionResponse resp1;
        std::string query1 = "DROP EDGE IF EXISTS exist_edge";
        auto code1 = client->execute(query1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);
    }
    // Show create edge
    std::string createEdgeStr;
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE education";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        createEdgeStr = "CREATE EDGE `education` (\n"
                        "  `school` int,\n"
                        "  `col1` int,\n"
                        "  `col2` string\n"
                        ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"education", createEdgeStr},
        };
        EXPECT_TRUE(verifyResult(resp, expected));
    }
    // Check resutl from show create
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE education";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        code = client->execute(createEdgeStr, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // show parts of default_space
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_NE(nullptr, resp.get_rows());
        auto row = resp.get_rows()->begin();
        GraphSpaceID spaceId = row->get_columns()[0].get_integer();
        auto kvstore = gEnv->storageServer()->kvStore_.get();
        nebula::storage::TestUtils::waitUntilAllElected(kvstore, spaceId, 9);
        // sleep a bit to make sure leader info has been updated in meta
        sleep(FLAGS_heartbeat_interval_secs + 1);

        query = "SHOW PARTS; # after leader election";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(9, (*(resp.get_rows())).size());
        std::string host = "127.0.0.1:" + std::to_string(gEnv->storageServerPort());
        std::vector<std::tuple<int, std::string, std::string, std::string>> expected;
        for (int32_t partId = 1; partId <= 9; partId++) {
            expected.emplace_back(std::make_tuple(partId, host, host, ""));
        }
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Tag with negative default value
    {
        cpp2::ExecutionResponse resp;
        const std::string cmd =
            "CREATE TAG default_tag_neg(id int DEFAULT -10, height double DEFAULT -176.0)";
        auto code = client->execute(cmd, resp);
        EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Tag with expression default value
    {
        cpp2::ExecutionResponse resp;
        const std::string cmd = "CREATE TAG default_tag_expr"
                                "(id int DEFAULT 3/2*4-5, "             // arithmetic
                                "male bool DEFAULT 3 > 2, "             // relation
                                "height double DEFAULT abs(-176.0), "   // built-in function
                                "adult bool DEFAULT true && false)";    // logic
        auto code = client->execute(cmd, resp);
        EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge with negative default value
    {
        cpp2::ExecutionResponse resp;
        const std::string cmd =
            "CREATE EDGE default_edge_neg(id int DEFAULT -10, height double DEFAULT -176.0)";
        auto code = client->execute(cmd, resp);
        EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge with expression default value
    {
        cpp2::ExecutionResponse resp;
        const std::string cmd = "CREATE EDGE default_edge_expr"
                                "(id int DEFAULT 3/2*4-5, "             // arithmetic
                                "male bool DEFAULT 3 > 2, "             // relation
                                "height double DEFAULT abs(-176.0), "   // built-in function
                                "adult bool DEFAULT true && false)";    // logic
        auto code = client->execute(cmd, resp);
        EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
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
        std::vector<SchemaTuple> expected{
            {"name", "string", false, "", "", ""},
            {"kind", "string", false, "", "", ""},
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
        std::vector<std::tuple<std::string>> expected{
            {"animal"},
            {"person"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
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
        std::vector<std::tuple<std::string>> expected1{
            {"test_tag"},
        };
        ASSERT_TRUE(verifyResult(resp, expected1, true, {0}));

        query = "USE test_multi; CREATE TAG test_tag1(); USE my_space; SHOW TAGS;";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected2{
            {"animal"},
            {"person"},
        };
        ASSERT_TRUE(verifyResult(resp, expected2, true, {0}));

        query = "DROP SPACE test_multi";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Test tagName is reserved keyword
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE my_space; CREATE TAG `tag` (`edge` string)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG `tag`";
        client->execute(query, resp);
        std::vector<SchemaTuple> expected{
            {"edge", "string", false, "", "", ""},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW SPACES";
        client->execute(query, resp);
        ASSERT_EQ(2, (*(resp.get_rows())).size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE my_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE default_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }

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

TEST_P(SchemaTestIssue1987, issue1987) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE " + GetParam()
            + " t(name string default \"N/A\", age int default -1)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // ADD
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER " + GetParam() + " t ADD (description string default \"none\");";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ::sleep(FLAGS_heartbeat_interval_secs + 1);
    }
    std::string ve;
    std::string entity;
    if (GetParam() == "TAG") {
        ve = "VERTEX";
        entity = "1";
    } else if (GetParam() == "EDGE") {
        ve = "EDGE";
        entity = "1->2";
    } else {
        ASSERT_TRUE(false) << "Invalid parameter " << GetParam();
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT " + ve + " t() VALUES " + entity + ":()";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    std::unordered_set<uint16_t> ignore;
    if (GetParam() == "TAG") {
        ignore.emplace(0);
    } else if (GetParam() == "EDGE") {
        ignore.emplace(0);
        ignore.emplace(1);
        ignore.emplace(2);
    } else {
        ASSERT_TRUE(false) << "Invalid parameter " << GetParam();
    }
    {
        std::vector<std::tuple<std::string, int64_t, std::string>> result {
            {"N/A", -1, "none"}
        };
        cpp2::ExecutionResponse resp;
        std::string query = std::string() + "FETCH PROP ON " + "t " + entity;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(verifyResult(resp, result, true, ignore));
    }

    // CHANGE
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER " + GetParam()
            + " t CHANGE (description string default \"NONE\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ::sleep(FLAGS_heartbeat_interval_secs + 1);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT " + ve + " t() VALUES " + entity + ":()";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        std::vector<std::tuple<std::string, int64_t, std::string>> result {
            {"N/A", -1, "NONE"}
        };
        cpp2::ExecutionResponse resp;
        std::string query = std::string() + "FETCH PROP ON " + "t " + entity;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(verifyResult(resp, result, true, ignore));
    }

    // DROP
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER " + GetParam() + " t CHANGE (description string)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ::sleep(FLAGS_heartbeat_interval_secs + 1);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT " + ve + " t(description) VALUES " + entity + ":(\"some one\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        std::vector<std::tuple<std::string, int64_t, std::string>> result {
            {"N/A", -1, "some one"}
        };
        cpp2::ExecutionResponse resp;
        std::string query = std::string() + "FETCH PROP ON " + "t " + entity;
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(verifyResult(resp, result, true, ignore));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT " + ve + " t() VALUES " + entity + ":()";
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

INSTANTIATE_TEST_CASE_P(SchemaIssue1987, SchemaTestIssue1987, ::testing::Values("TAG", "EDGE"));


TEST_F(SchemaTest, issue2009) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE issue2009; USE issue2009";
        auto code = client->execute(query, resp);
        ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE IF NOT EXISTS relation"
                            "(intimacy int default 0, "
                            "isReversible bool default false, "
                            "name string default \"N/A\", "
                            "startTime timestamp default 0)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);
    }
    ::sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT EDGE relation (intimacy) VALUES "
                            "hash(\"person.Tom\") -> hash(\"person.Marry\")@0:(3)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(code, cpp2::ErrorCode::SUCCEEDED);
    }
}

TEST_F(SchemaTest, DescribeSchema) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE describe_schema";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "USE describe_schema";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }

    // Tag Schema
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG tag_test(id int DEFAULT 233, name string DEFAULT 'shylock')";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE TAG INDEX tag_test_index on tag_test(id, name)";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DESC TAG tag_test";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<SchemaTuple> expected{
            {"id", "int", false, "MUL", "233", ""},
            {"name", "string", false, "", "\"shylock\"", ""},
        };
        EXPECT_TRUE(verifyResult(resp, expected));
    }

    // Edge Schema
    {
        cpp2::ExecutionResponse resp;
        std::string cmd =
            "CREATE EDGE edge_test(id int DEFAULT 233, name string DEFAULT 'shylock')";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE EDGE INDEX edge_test_index on edge_test(id, name)";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "DESC EDGE edge_test";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<SchemaTuple> expected{
            {"id", "int", false, "MUL", "233", ""},
            {"name", "string", false, "", "\"shylock\"", ""},
        };
        EXPECT_TRUE(verifyResult(resp, expected));
    }
}

}   // namespace graph
}   // namespace nebula
