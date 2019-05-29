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
class UserTest : public TestBase {
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

TEST_F(UserTest, userManagerTest) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER IF NOT EXISTS user1 WITH PASSWORD \"aaa\" , "
                            "ACCOUNT LOCK, MAX_QUERIES_PER_HOUR 1, "
                            "MAX_UPDATES_PER_HOUR 2, MAX_CONNECTIONS_PER_HOUR 3, "
                            "MAX_USER_CONNECTIONS 4";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Test 'IF NOT EXISTS' when user exists. expected SUCCEEDED.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER IF NOT EXISTS user1 WITH PASSWORD \"aaa\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Test user exists and 'IF NOT EXISTS' is not specified. expected failing.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER user1 WITH PASSWORD \"aaa\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Test each option property.
    {
        cpp2::ExecutionResponse resp;
        std::string query1 = "CREATE USER user2 WITH PASSWORD \"aaa\" , "
                            "MAX_QUERIES_PER_HOUR 11";
        std::string query2 = "CREATE USER user3 WITH PASSWORD \"aaa\" , "
                            "MAX_UPDATES_PER_HOUR 22";
        std::string query3 = "CREATE USER user4 WITH PASSWORD \"aaa\" , "
                             "MAX_CONNECTIONS_PER_HOUR 33";
        std::string query4 = "CREATE USER user5 WITH PASSWORD \"aaa\" , "
                             "MAX_USER_CONNECTIONS 44";
        std::string query5 = "CREATE USER user6 WITH PASSWORD \"aaa\" , "
                             "ACCOUNT UNLOCK";
        std::string query6 = "CREATE USER user7 WITH PASSWORD \"aaa\" , "
                             "ACCOUNT LOCK";
        auto code = client->execute(query1, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        code = client->execute(query2, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        code = client->execute(query3, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        code = client->execute(query4, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        code = client->execute(query5, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        code = client->execute(query6, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_load_data_interval_secs + 1);
    // Test list users.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW USERS";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 6>> expected{
                {"user1", "locked", "1", "2", "3", "4"},
                {"user2", "unlock", "11", "0", "0", "0"},
                {"user3", "unlock", "0", "22", "0", "0"},
                {"user4", "unlock", "0", "0", "33", "0"},
                {"user5", "unlock", "0", "0", "0", "44"},
                {"user6", "unlock", "0", "0", "0", "0"},
                {"user7", "locked", "0", "0", "0", "0"},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test alter user by each option property.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER USER user1 WITH ACCOUNT UNLOCK, "
                            "MAX_QUERIES_PER_HOUR 55";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::string show = "SHOW USER user1";
        client->execute(show, resp);
        std::vector<uniform_tuple_t<std::string, 6>> expected{
                {"user1", "unlock", "55", "2", "3", "4"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    // Test Drop user.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP USER user4";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "SHOW USER user4";
        code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
        // Test 'IF EXISTS' when user did not exists. expected SUCCEEDED.
        query = "DROP USER IF EXISTS user4";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        // Test without 'IF EXISTS' when user did not exists. expected failing.
        query = "DROP USER user4";
        code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Setup spaces.
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
        std::string query = "CREATE SPACE space_1(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "CREATE SPACE space_2(partition_num=1, replica_factor=1)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_load_data_interval_secs + 1);
    // Test Grant and Revoke.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE ADMIN ON space_1 TO user1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "GRANT ROLE USER ON space_1 TO user2";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "GRANT ROLE GUEST ON space_1 TO user3";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "GRANT ROLE GUEST ON space_2 TO user5";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        query = "REVOKE ROLE GUEST ON space_2 FROM user5";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_load_data_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW ROLES IN space_1";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> expected{
                {"user1", "admin"},
                {"user2", "user"},
                {"user3", "guest"}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        query = "SHOW ROLES IN space_2";
        client->execute(query, resp);
        std::vector<uniform_tuple_t<std::string, 2>> nullResult{};
        ASSERT_TRUE(verifyResult(resp, nullResult));
    }
    // Test change password.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CHANGE PASSWORD user1 FROM \"bbb\" TO \"aaa\"";
        auto code = client->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
        query = "CHANGE PASSWORD user1 FROM \"aaa\" TO \"bbb\"";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}
}   // namespace graph
}   // namespace nebula

