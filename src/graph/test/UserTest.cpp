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

TEST_F(UserTest, CreateUser) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER user1 WITH PASSWORD \"pwd1\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER user2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // user exists.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER user1 WITH PASSWORD \"pwd1\" ";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // user exists. but if not exists is true.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER IF NOT EXISTS user1 WITH PASSWORD \"pwd1\" ";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW USERS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(3, resp.get_rows()->size());
        decltype(resp.column_names) colNames = {"Account"};
        ASSERT_TRUE(verifyColNames(resp, colNames));

        std::vector<std::tuple<std::string>>
        rows = {{"root"}, {"user1"}, {"user2"}, };
        ASSERT_TRUE(verifyResult(resp, rows));
    }
}

TEST_F(UserTest, AlterUser) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER USER user WITH PASSWORD \"pwd1\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER USER user2 WITH PASSWORD \"pwd2\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(UserTest, DropUser) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP USER user";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP USER IF EXISTS user";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER user3";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP USER user3";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW USERS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(3, resp.get_rows()->size());
    }
}

TEST_F(UserTest, ChangePassword) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    // user is not exists. expect fail.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CHANGE PASSWORD user FROM \"pwd\" TO \"pwd\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // user login type is LDAP, can not change password. expect fail.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CHANGE PASSWORD user1 FROM \"pwd\" TO \"pwd\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // old password invalid. expect fail.
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CHANGE PASSWORD user2 FROM \"pwd\" TO \"newpwd\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CHANGE PASSWORD user2 FROM \"pwd2\" TO \"newpwd\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(UserTest, GrantRevoke) {
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "CREATE SPACE user_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    // must set the space if is not god role. expect fail.
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GRANT DBA TO user1";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // user not exist. expect fail.
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GRANT DBA ON user_space TO user";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GRANT GOD ON user_space TO user1";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    // space not exists. expect fail.
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GRANT ROLE DBA ON space TO user2";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW USERS";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(3, resp.get_rows()->size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GRANT ROLE DBA ON user_space TO user2";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER user3";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "GRANT ROLE GUEST ON user_space TO user3";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // space not exists. expect fail.
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "SHOW ROLES IN space";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "SHOW ROLES IN user_space";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(2, resp.get_rows()->size());
        decltype(resp.column_names) colNames = {
            "Account",
            "Role Type"
        };
        ASSERT_TRUE(verifyColNames(resp, colNames));

        std::vector<std::tuple<std::string, std::string>>
            rows = {
            {"user2", "DBA"},
            {"user3", "GUEST"}
        };
        ASSERT_TRUE(verifyResult(resp, rows));
    }
    // space not exist, expect fail.
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "REVOKE ROLE DBA ON space FROM user2";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // user not exist. expect fail.
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "REVOKE ROLE DBA ON user_space FROM user";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    // role invalid. expect fail.
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "REVOKE ROLE ADMIN ON user_space FROM user2";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "REVOKE ROLE DBA ON user_space FROM user2";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string cmd = "SHOW ROLES IN user_space";
        auto code = client->execute(cmd, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(1, resp.get_rows()->size());
        std::vector<std::tuple<std::string, std::string>>
            rows = {
            {"user3", "GUEST"}
        };
        ASSERT_TRUE(verifyResult(resp, rows));
    }
}

}   // namespace graph
}   // namespace nebula
