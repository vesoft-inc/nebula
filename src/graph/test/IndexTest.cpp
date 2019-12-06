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
        std::string query = "CREATE SPACE index_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
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
        std::string query = "CREATE TAG INDEX ON person single_person_index(name)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Space not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX ON student single_person_index(name)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Property not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX ON person single_person_index(phone)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Single Tag Multi Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX ON person multi_person_index(name, email)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Multi Tag Multi Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX ON person,course person_course_index(name, teacher)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX ON person,course "
                            "specified_tag_index(person.name, course.teacher)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Describe Tag Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE TAG INDEX person_course_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // List Tag Indexes
    {
        cpp2::ExecutionResponse resp;
        std::string query = "LIST TAG INDEXES";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop Tag Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG INDEX person_course_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "DESCRIBE TAG INDEX person_course_index";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(IndexTest, EdgeIndex) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE index_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
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
        std::string query = "CREATE EDGE INDEX ON friend single_friend_index(degree)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Space not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX ON friendship single_person_index(name)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Property not exist
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX ON friend single_person_index(startTime)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // Single EDGE Multi Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX ON friend multi_friend_index(degree, start_time)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Multi EDGE Multi Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX ON friend,transfer "
                            "friend_transfer_index(degree, amount)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE INDEX ON friend,transfer "
                            "specified_edge_index(friend.degree, transfer.amount)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Describe Edge Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESCRIBE EDGE INDEX friend_transfer_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // List Edge Indexes
    {
        cpp2::ExecutionResponse resp;
        std::string query = "LIST EDGE INDEXES";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Drop Edge Index
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE INDEX friend_transfer_index";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

}   // namespace graph
}   // namespace nebula

