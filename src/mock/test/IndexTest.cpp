/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "mock/test/TestEnv.h"
#include "mock/test/TestBase.h"
#include <gtest/gtest.h>

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class IndexTest : public TestBase {
protected:
    void SetUp() override {
        TestBase::SetUp();
    }

    void TearDown() override {
        TestBase::TearDown();
    }

    static void SetUpTestCase() {
        client_ = gEnv->getGraphClient();
        ASSERT_NE(nullptr, client_);
    }

    static void TearDownTestCase() {
        client_.reset();
    }

protected:
    static std::unique_ptr<GraphClient>     client_;
};

std::unique_ptr<GraphClient> IndexTest::client_{nullptr};

TEST_F(IndexTest, TagIndex) {
    ASSERT_NE(nullptr, client_);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE tag_index_space(partition_num=1, replica_factor=1)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE tag_index_space";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG tag_1(col1 string, col2 int, col3 double, col4 timestamp)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);

    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW TAGS;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"Name"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {{"tag_1"}};
        ASSERT_TRUE(verifyValues(resp, values));
    }

    // Single Tag Single Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_tag_index ON tag_1(col2)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Property is empty
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX single_tag_index ON tag_1()";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Single Tag Multi Field
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX multi_tag_index ON tag_1(col2, col3)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG INDEX disorder_tag_index ON tag_1(col3, col2)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG INDEX IF EXISTS not_exists_tag_index";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE tag_index_space";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}


}   // namespace graph
}   // namespace nebula

