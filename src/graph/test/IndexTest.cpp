/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"

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

        query = "CREATE TAG person(name string, age int, gender string, email string)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG course(teacher string, score double)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Single Tag Single Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_person_index ON person(name)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
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
        std::string query = "CREATE TAG INDEX single_person_index ON person(phone)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Property is empty
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_person_index ON person()";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Single Tag Multi Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX multi_person_index ON person(name, email)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Describe Tag Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG INDEX multi_person_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DESC TAG INDEX multi_person_index";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"name",  "string"},
            {"email", "string"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Show Create Tag Indexes
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG INDEX multi_person_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string createTagIndex = "CREATE TAG INDEX multi_person_index ON person(name, email)";
        std::vector<std::tuple<std::string, std::string>> expected{
            {"multi_person_index", createTagIndex},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // List Tag Indexes
    {
        cpp2::ExecutionResponse resp;
        std::string query = "Show TAG INDEXES";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int32_t, std::string>> expected{
            {4, "single_person_index"},
            {5, "multi_person_index"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Drop Tag Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG INDEX multi_person_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DESCRIBE TAG INDEX multi_person_index";
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

        query = "CREATE EDGE friend(degree string, start_time int)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE transfer(amount double, bank string)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Single Edge Single Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX single_friend_index ON friend(degree)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX single_person_index ON friendship(name)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Property not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX single_person_index ON friend(startTime)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Property is empty
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX single_person_index ON friend()";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Single EDGE Multi Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX multi_friend_index ON friend(degree, start_time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Describe Edge Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE INDEX multi_friend_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DESC EDGE INDEX multi_friend_index";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
            {"degree",     "string"},
            {"start_time", "int"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Show Create Edge Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE INDEX multi_friend_index";
        auto code = client->execute(query, resp);
        std::string createTagIndex = "CREATE EDGE INDEX multi_friend_index ON "
                                     "friend(degree, start_time)";
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<std::string, std::string>> expected{
            {"multi_friend_index", createTagIndex},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // List Edge Indexes
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW EDGE INDEXES";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<int32_t, std::string>> expected{
            {9, "single_friend_index"},
            {10, "multi_friend_index"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Drop Edge Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE INDEX multi_friend_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DESCRIBE EDGE INDEX multi_friend_index";
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

        query = "CREATE TAG person_ttl(name string, age int, gender int, email string)";
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
        std::string query = "CREATE TAG person_ttl_2(name string, age int, gender string)"
                " ttl_duration = 200, ttl_col = \"age\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Tag with ttl cannot create index on not ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX person_ttl_2_index ON person_ttl_2(name)";
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
        std::string query = "CREATE TAG INDEX person_ttl_2_index ON person_ttl_2(name)";
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

        query = "CREATE EDGE friend_ttl(degree int, start_time int)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Single Edge Single Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX single_friend_ttl_index ON friend_ttl(start_time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter edge add ttl property on index col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER edge friend_ttl ttl_duration = 100, ttl_col = \"start_time\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter edge add ttl property on not index col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER edge friend_ttl ttl_duration = 100, ttl_col = \"degree\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE INDEX single_friend_ttl_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter edge add ttl property on index col, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER edge friend_ttl ttl_duration = 100, ttl_col = \"start_time\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Alter edge add ttl property on not index col, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER edge friend_ttl ttl_duration = 100, ttl_col = \"degree\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge with ttl to create index on ttl col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX friend_ttl_index_second ON friend_ttl(degree)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge with ttl to create index on no ttl col, failed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX friend_ttl_index_second ON friend_ttl(start_time)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop ttl propery
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE friend_ttl ttl_col = \"\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge without ttl to create index, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX friend_ttl_index_second ON friend_ttl(start_time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE INDEX friend_ttl_index_second";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }

    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE friend_ttl_2(degree int, start_time int)"
                " ttl_duration = 200, ttl_col = \"start_time\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge with ttl cannot create index on not ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX friend_ttl_index_2 ON friend_ttl_2(degree)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Edge with ttl cannot create index on ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX friend_ttl_index_2 ON friend_ttl_2(start_time)";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop ttl col
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE friend_ttl_2 DROP (start_time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Create index, succeed
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX friend_ttl_index_2 ON friend_ttl_2(degree)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
    // Drop index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE INDEX friend_ttl_index_2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE friend_ttl_2";
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

}   // namespace graph
}   // namespace nebula

