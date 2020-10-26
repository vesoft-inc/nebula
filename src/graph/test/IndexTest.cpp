/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class IndexTest : public TestBase {
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

TEST_F(IndexTest, TagIndex) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE tag_index_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "USE tag_index_space";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG tag_1(col1 string, col2 int, col3 double, col4 timestamp)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    // Single Tag Single Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_tag_index ON tag_1(col2)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX duplicate_tag_index_1 ON tag_1(col2)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Tag not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_person_index ON student(name)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Property not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_tag_index ON tag_1(col5)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Property is empty
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_tag_index ON tag_1()";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Single Tag Multi Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX multi_tag_index ON tag_1(col2, col3)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX duplicate_person_index ON tag_1(col2, col3)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX duplicate_index ON tag_1(col2, col2)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX disorder_tag_index ON tag_1(col3, col2)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX tag_1(col1, col2, col3, col4) VALUES "
                     "uuid(\"Tim\"):  (\"Tim\",  18, 11.11, \"2000-10-10 10:00:00\"), "
                     "uuid(\"Tony\"): (\"Tony\", 18, 11.11, \"2000-10-10 10:00:00\"), "
                     "uuid(\"May\"):  (\"May\",  18, 11.11, \"2000-10-10 10:00:00\"), "
                     "uuid(\"Tom\"):  (\"Tom\",  18, 11.11, \"2000-10-10 10:00:00\")";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    // Rebuild Tag Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REBUILD TAG INDEX single_tag_index OFFLINE";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REBUILD TAG INDEX single_tag_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REBUILD TAG INDEX multi_tag_index OFFLINE";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REBUILD TAG INDEX multi_tag_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    // Show Tag Index Status
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW TAG INDEX STATUS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        /*
         * Currently , expected the index status is "RUNNING" or "SUCCEEDED"
         */
        for (auto& row : *resp.get_rows()) {
            const auto &columns = row.get_columns();
            ASSERT_NE("FAILED", columns[1].get_str());
        }
    }
    // Describe Tag Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG INDEX multi_tag_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DESC TAG INDEX multi_tag_index";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"col2",  "int"},
            {"col3", "double"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Show Create Tag Indexes
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG INDEX multi_tag_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagIndex = "CREATE TAG INDEX `multi_tag_index` "
                                     "ON `tag_1`(`col2`, `col3`)";
        std::vector<std::tuple<std::string, std::string>> expected{
            {"multi_tag_index", createTagIndex},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        query = "DROP TAG INDEX multi_tag_index;" + createTagIndex;
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // List Tag Indexes
    {
        cpp2::ExecutionResponse resp;
        std::string query = "Show TAG INDEXES";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected{
            {"single_tag_index"},
            {"multi_tag_index"},
            {"disorder_tag_index"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    // Drop Tag Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG INDEX multi_tag_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DESCRIBE TAG INDEX multi_tag_index";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG INDEX not_exists_tag_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG INDEX IF EXISTS not_exists_tag_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE tag_index_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}


TEST_F(IndexTest, EdgeIndex) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE edge_index_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "USE edge_index_space";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE edge_1(col1 string, col2 int, col3 double, col4 timestamp)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    // Single Edge Single Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX single_edge_index ON edge_1(col2)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX duplicate_edge_1_index ON edge_1(col2)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Edge not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX single_edge_index ON edge_1_ship(name)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Property not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX single_edge_index ON edge_1(startTime)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Property is empty
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX single_edge_index ON edge_1()";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Single EDGE Multi Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX multi_edge_1_index ON edge_1(col2, col3)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX duplicate_edge_1_index "
                            "ON edge_1(col2, col3)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX duplicate_index ON edge_1(col2, col2)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX disorder_edge_1_index ON edge_1(col3, col2)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE edge_1(col1, col2, col3, col4) VALUES "
                     "uuid(\"Tim\")  -> uuid(\"May\"):  (\"Good\", 18, 11.11, \"2000-10-10 10:00:00\"), "
                     "uuid(\"Tim\")  -> uuid(\"Tony\"): (\"Good\", 18, 11.11, \"2000-10-10 10:00:00\"), "
                     "uuid(\"Tony\") -> uuid(\"May\"):  (\"Like\", 18, 11.11, \"2000-10-10 10:00:00\"), "
                     "uuid(\"May\")  -> uuid(\"Tim\"):  (\"Like\", 18, 11.11, \"2000-10-10 10:00:00\")";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    // Rebuild EDGE Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REBUILD EDGE INDEX single_edge_index OFFLINE";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REBUILD EDGE INDEX single_edge_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REBUILD EDGE INDEX multi_edge_1_index OFFLINE";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REBUILD EDGE INDEX multi_edge_1_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    // Show EDGE Index Status
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW EDGE INDEX STATUS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        /*
         * Currently , expected the index status is "RUNNING" or "SUCCEEDED"
         */
        for (auto& row : *resp.get_rows()) {
            const auto &columns = row.get_columns();
            ASSERT_NE("FAILED", columns[1].get_str());
        }
    }
    // Describe Edge Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE INDEX multi_edge_1_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DESC EDGE INDEX multi_edge_1_index";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"col2", "int"},
            {"col3", "double"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Show Create Edge Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE INDEX multi_edge_1_index";
        auto code = client->execute(query, resp);
        std::string createEdgeIndex = "CREATE EDGE INDEX `multi_edge_1_index` ON "
                                      "`edge_1`(`col2`, `col3`)";
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string>> expected{
            {"multi_edge_1_index", createEdgeIndex},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        query = "DROP EDGE INDEX multi_edge_1_index;" + createEdgeIndex;
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // List Edge Indexes
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW EDGE INDEXES";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string>> expected{
            {"single_edge_index"},
            {"multi_edge_1_index"},
            {"disorder_edge_1_index"},
        };
        ASSERT_TRUE(verifyResult(resp, expected, true, {0}));
    }
    // Drop Edge Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE INDEX multi_edge_1_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DESCRIBE EDGE INDEX multi_edge_1_index";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE INDEX not_exists_edge_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE INDEX IF EXISTS not_exists_edge_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE edge_index_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}


TEST_F(IndexTest, TagIndexTTL) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE tag_ttl_index_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "USE tag_ttl_index_space";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG person_ttl(number int, age int, gender int, email string)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Single Tag Single Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_person_ttl_index ON person_ttl(age)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter tag add ttl property on index col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG person_ttl ttl_duration = 100, ttl_col = \"age\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter tag add ttl property on not index col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG person_ttl ttl_duration = 100, ttl_col = \"gender\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG INDEX single_person_ttl_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter tag add ttl property on index col, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG person_ttl ttl_duration = 100, ttl_col = \"age\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter tag add ttl property on not index col, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG person_ttl ttl_duration = 100, ttl_col = \"gender\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Tag with ttl to create index on ttl col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_person_ttl_index_second ON person_ttl(gender)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Tag with ttl to create index on not ttl col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_person_ttl_index_second ON person_ttl(age)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop ttl propery
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG person_ttl  ttl_col = \"\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Tag with ttl to create index, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_person_ttl_index_second ON person_ttl(age)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG INDEX single_person_ttl_index_second";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }

    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG person_ttl_2(number int, age int, gender string) "
                            "ttl_duration = 200, ttl_col = \"age\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Tag with ttl cannot create index on not ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX person_ttl_2_index ON person_ttl_2(number)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Tag with ttl cannot create index on ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX person_ttl_2_index ON person_ttl_2(age)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG person_ttl_2 DROP (age)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Create index, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX person_ttl_2_index ON person_ttl_2(number)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG INDEX person_ttl_2_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE tag_ttl_index_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}


TEST_F(IndexTest, EdgeIndexTTL) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE edge_ttl_index_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "USE edge_ttl_index_space";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE edge_1_ttl(degree int, start_time int)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Single Edge Single Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX single_edge_1_ttl_index ON edge_1_ttl(start_time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter edge add ttl property on index col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER edge edge_1_ttl ttl_duration = 100, ttl_col = \"start_time\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter edge add ttl property on not index col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER edge edge_1_ttl ttl_duration = 100, ttl_col = \"degree\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE INDEX single_edge_1_ttl_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter edge add ttl property on index col, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER edge edge_1_ttl ttl_duration = 100, ttl_col = \"start_time\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter edge add ttl property on not index col, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER edge edge_1_ttl ttl_duration = 100, ttl_col = \"degree\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge with ttl to create index on ttl col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX edge_1_ttl_index_second ON edge_1_ttl(degree)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge with ttl to create index on no ttl col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX edge_1_ttl_index_second ON edge_1_ttl(start_time)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop ttl propery
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE edge_1_ttl ttl_col = \"\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge without ttl to create index, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX edge_1_ttl_index_second ON edge_1_ttl(start_time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE INDEX edge_1_ttl_index_second";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE edge_1_ttl_2(degree int, start_time int) "
                            "ttl_duration = 200, ttl_col = \"start_time\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge with ttl cannot create index on not ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX edge_1_ttl_index_2 ON edge_1_ttl_2(degree)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge with ttl cannot create index on ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX edge_1_ttl_index_2 ON edge_1_ttl_2(start_time)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE edge_1_ttl_2 DROP (start_time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Create index, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX edge_1_ttl_index_2 ON edge_1_ttl_2(degree)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
    // Drop index
        {
            cpp2::ExecutionResponse resp;
            std::string query = "DROP EDGE INDEX edge_1_ttl_index_2";
            auto code = client->execute(query, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        }
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE edge_1_ttl_2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE edge_ttl_index_space";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(IndexTest, AlterTag) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE tag_index_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "USE tag_index_space";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG tag_1(col1 bool, col2 int, col3 double, col4 timestamp)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    // Single Tag Single Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_person_index ON tag_1(col1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX tag_1(col1, col2, col3, col4) VALUES "
                     "100:  (true,  18, 1.1, \"2000-10-10 10:00:00\")";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "ALTER TAG tag_1 ADD (col5 int)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    // Single Tag Single Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_person_index2 ON tag_1(col5)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX tag_1(col1, col2, col3, col4, col5) VALUES "
                     "100:(true,  18, 1.1, \"2000-10-10 10:00:00\", 5)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON tag_1 WHERE tag_1.col5 == 5 YIELD tag_1.col5, tag_1.col1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, int64_t, bool>> expected = {
            {100, 5, true},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON tag_1 where tag_1.col1 == true YIELD tag_1.col1, tag_1.col5";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, bool, int64_t>> expected = {
            {100, true, 5},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

bool verifyIndexStatus(GraphClient* client , bool isEdge) {
    for (auto retry = 1; retry <= 3; retry++) {
        cpp2::ExecutionResponse resp;
        std::string query = isEdge ? "SHOW EDGE INDEX STATUS" : "SHOW TAG INDEX STATUS";
        auto code = client->execute(query, resp);
        if (cpp2::ErrorCode::SUCCEEDED != code) {
            return false;
        }
        auto rows = *resp.get_rows();
        const auto &columns = rows[0].get_columns();
        if ("RUNNING" != columns[1].get_str()) {
            return true;
        }
        sleep(1);
    }
    return false;
}

TEST_F(IndexTest, RebuildTagIndexStatusInfo) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE tag_status_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "USE tag_status_space";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG tag_status(col1 int)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX tag_index_status ON tag_status(col1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW TAG INDEX STATUS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(0, resp.get_rows()->size());
    }
    // Rebuild Tag Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REBUILD TAG INDEX tag_index_status OFFLINE";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        ASSERT_TRUE(verifyIndexStatus(client.get(), false));
    }
    // Drop Tag Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG INDEX tag_index_status";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DESCRIBE TAG INDEX tag_index_status";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    // rebuild index status deleted.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW TAG INDEX STATUS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(0, resp.get_rows()->size());
    }
}

TEST_F(IndexTest, RebuildEdgeIndexStatusInfo) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE edge_status_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "USE edge_status_space";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE edge_status(col1 int)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX edge_index_status ON edge_status(col1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW EDGE INDEX STATUS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(0, resp.get_rows()->size());
    }
    // Rebuild Edge Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "REBUILD EDGE INDEX edge_index_status OFFLINE";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        ASSERT_TRUE(verifyIndexStatus(client.get(), true));
    }
    // Drop Edge Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE INDEX edge_index_status";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DESCRIBE EDGE INDEX edge_index_status";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    // rebuild index status deleted.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW EDGE INDEX STATUS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(0, resp.get_rows()->size());
    }
}

TEST_F(IndexTest, AlterSchemaTest) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE alter_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "USE alter_space";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG alter_tag(id int)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX alter_index ON alter_tag(id)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX alter_tag(id) VALUES "
                     "100:(1), 200:(2)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "ALTER TAG alter_tag ADD (type int)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON alter_tag WHERE alter_tag.id == 1 YIELD alter_tag.type";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
}

TEST_F(IndexTest, IgnoreExistedIndexTest) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE ingore_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "USE ingore_space";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG person(id int)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG INDEX id_index ON person(id)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        // insert some data
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX person(id) VALUES 100:(1), 200:(1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // lookup id == 1, both 100 and 200 satisfy
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON person WHERE person.id == 1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {{100}, {200}};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // overwrite tag of 100
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX person(id) VALUES 100:(1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // overwrite tag of 200 with ignore_existed_index. Old index of 200 will not be deleted
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX IGNORE_EXISTED_INDEX person(id) VALUES 200:(2)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // lookup id == 1, both 100 and 200 satisfy.
        // the old index of 200 still exists
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON person WHERE person.id == 1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {{100}, {200}};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // lookup id == 2, 200 satisfy.
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON person WHERE person.id == 2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {{200}};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "CREATE EDGE like(grade int)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE INDEX grade_index ON like(grade)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        // insert some data
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE like(grade) VALUES 100 -> 200:(666), 300 -> 400:(666);";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // lookup grade == 666, both edge satisfy
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON like WHERE like.grade == 666";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected =
            {{300, 400, 0}, {100, 200, 0}};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // overwrite edge of 100 -> 200
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE like(grade) VALUES 100 -> 200:(666)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // overwrite edge of 300 -> 400 with ignore_existed_index
        // old index of the edge will not be deleted
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE IGNORE_EXISTED_INDEX like(grade) VALUES 300 -> 400:(888)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        // lookup grade == 666, both edge satisfy
        // the old index of 300 -> 400 still exists
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON like WHERE like.grade == 666";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {300, 400, 0}, {100, 200, 0}};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        // lookup grade == 888, 300 -> 400 satisfy.
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON like WHERE like.grade == 888";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {{300, 400, 0}};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

}   // namespace graph
}   // namespace nebula

