/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class PermissionTest : public TestBase {
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

TEST_F(PermissionTest, SimpleTest) {
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_enable_authorize = true;
    /*
     * test incorrect password.
     */
    {
        auto client = gEnv->getClient("root", "pwd");
        ASSERT_EQ(nullptr, client);
    }
    /*
     * test incorrect user name.
     */
    {
        auto client = gEnv->getClient("user", "nebula");
        ASSERT_EQ(nullptr, client);
    }
    /*
     * test root user password and use space.
     */
    {
        auto client = gEnv->getClient();
        ASSERT_NE(nullptr, client);
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE my_space(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "USE my_space; CREATE TAG person(name string)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        client->disconnect();
    }
    /*
     * change root password, incorrect password.
     */
    {
        auto client = gEnv->getClient();
        ASSERT_NE(nullptr, client);
        cpp2::ExecutionResponse resp;
        std::string query = "CHANGE PASSWORD root FROM \"aa\" TO \"bb\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
        client->disconnect();
    }
    /*
     * change root password, correct password.
     */
    {
        auto client = gEnv->getClient();
        ASSERT_NE(nullptr, client);
        cpp2::ExecutionResponse resp;
        std::string query = "CHANGE PASSWORD root FROM \"nebula\" TO \"bb\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        client->disconnect();
    }
    /*
     * verify password changed
     */
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        auto client = gEnv->getClient("root", "nebula");
        ASSERT_EQ(nullptr, client);
    }
    {
        auto client = gEnv->getClient("root", "bb");
        ASSERT_NE(nullptr, client);
        cpp2::ExecutionResponse resp;
        std::string query = "CHANGE PASSWORD root FROM \"bb\" TO \"nebula\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        client->disconnect();
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
}

TEST_F(PermissionTest, UserWriteTest) {
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_enable_authorize = true;
    sleep(FLAGS_heartbeat_interval_secs + 1);
    auto client = gEnv->getClient();
    ASSERT_NE(nullptr, client);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space1(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER admin WITH PASSWORD \"admin\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE ADMIN ON space1 TO admin";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE GOD ON space1 TO admin";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW ROLES IN space1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(1, resp.get_rows()->size());
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        auto adminClient = gEnv->getClient("admin", "admin");
        ASSERT_NE(nullptr, adminClient);
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER USER root WITH PASSWORD \"root\"";
        auto code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    {
        auto adminClient = gEnv->getClient("admin", "admin");
        ASSERT_NE(nullptr, adminClient);
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE ADMIN ON space1 TO admin";
        auto code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    {
        auto adminClient = gEnv->getClient("admin", "admin");
        ASSERT_NE(nullptr, adminClient);
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE GOD ON space1 TO admin";
        auto code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    {
        auto adminClient = gEnv->getClient("admin", "admin");
        ASSERT_NE(nullptr, adminClient);
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE GOD ON space1 TO admin";
        auto code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    /**
     * Reject the admin user grant or revoke to himself self
     */
    {
        auto adminClient = gEnv->getClient("admin", "admin");
        ASSERT_NE(nullptr, adminClient);
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE GUEST ON space1 TO admin";
        auto code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    {
        auto adminClient = gEnv->getClient("admin", "admin");
        ASSERT_NE(nullptr, adminClient);
        cpp2::ExecutionResponse resp;
        std::string query = "DROP USER admin";
        auto code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP USER admin";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(PermissionTest, SchemaAndDataTest) {
    FLAGS_enable_authorize = true;
    auto client = gEnv->getClient();
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space2(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER admin WITH PASSWORD \"admin\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE ADMIN ON space2 TO admin";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER dba WITH PASSWORD \"dba\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE DBA ON space2 TO dba";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER user WITH PASSWORD \"user\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE USER ON space2 TO user";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER guest WITH PASSWORD \"guest\"";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE GUEST ON space2 TO guest";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    /**
     * god write schema test
     */
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space2";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG t1(t_c int)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE e1(e_c int)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE TAG t1";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE EDGE e1";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE TAG INDEX tid1";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE EDGE INDEX eid1";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP TAG INDEX tid1";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP EDGE INDEX eid1";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "ALTER TAG t1 DROP (t_c)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "ALTER EDGE e1 DROP (e_c)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP TAG t1";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP EDGE e1";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    /**
     * admin write schema test
     */
    sleep(FLAGS_heartbeat_interval_secs + 1);
    auto adminClient = gEnv->getClient("admin", "admin");
    ASSERT_NE(nullptr, adminClient);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space2";
        auto code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG t1(t_c int)";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE e1(e_c int)";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE TAG t1";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE EDGE e1";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE TAG INDEX tid1";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE EDGE INDEX eid1";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP TAG INDEX tid1";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP EDGE INDEX eid1";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "ALTER TAG t1 DROP (t_c)";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "ALTER EDGE e1 DROP (e_c)";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP TAG t1";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP EDGE e1";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }

    /**
     * dba write schema test
     */
    sleep(FLAGS_heartbeat_interval_secs + 1);
    auto dbaClient = gEnv->getClient("dba", "dba");
    ASSERT_NE(nullptr, dbaClient);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space2";
        auto code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG t1(t_c int)";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE e1(e_c int)";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE TAG t1";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE EDGE e1";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE TAG INDEX tid1";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DESCRIBE EDGE INDEX eid1";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP TAG INDEX tid1";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP EDGE INDEX eid1";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "ALTER TAG t1 DROP (t_c)";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "ALTER EDGE e1 DROP (e_c)";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP TAG t1";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "DROP EDGE e1";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }

    /**
     * user write schema test
     */
    sleep(FLAGS_heartbeat_interval_secs + 1);
    auto userClient = gEnv->getClient("user", "user");
    ASSERT_NE(nullptr, userClient);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space2";
        auto code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG t1(t_c int)";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "CREATE EDGE e1(e_c int)";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "DESCRIBE TAG t1";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);

        query = "DESCRIBE EDGE e1";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);

        query = "DESCRIBE TAG INDEX tid1";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);

        query = "DESCRIBE EDGE INDEX eid1";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);

        query = "DROP TAG INDEX tid1";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "DROP EDGE INDEX eid1";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "ALTER TAG t1 DROP (t_c)";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "ALTER EDGE e1 DROP (e_c)";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "DROP TAG t1";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "DROP EDGE e1";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }

    /**
     * guest write schema test
     */
    sleep(FLAGS_heartbeat_interval_secs + 1);
    auto guestClient = gEnv->getClient("guest", "guest");
    ASSERT_NE(nullptr, guestClient);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space2";
        auto code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE TAG t1(t_c int)";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "CREATE EDGE e1(e_c int)";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "CREATE TAG INDEX tid1 ON t1(t_c)";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "CREATE EDGE INDEX eid1 ON e1(e_c)";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "DESCRIBE TAG t1";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);

        query = "DESCRIBE EDGE e1";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);

        query = "DESCRIBE TAG INDEX tid1";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);

        query = "DESCRIBE EDGE INDEX eid1";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);

        query = "DROP TAG INDEX tid1";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "DROP EDGE INDEX eid1";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "ALTER TAG t1 DROP (t_c)";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "ALTER EDGE e1 DROP (e_c)";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "DROP TAG t1";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "DROP EDGE e1";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    /**
     * god write data test
     */
    {
        cpp2::ExecutionResponse resp;
        auto query = "CREATE TAG t1(t_c int)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "CREATE EDGE e1(e_c int)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        sleep(FLAGS_heartbeat_interval_secs + 1);

        query = "INSERT VERTEX t1(t_c) VALUES 1:(1)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "INSERT EDGE e1(e_c) VALUES 1 -> 2:(95)";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "GO FROM 1 OVER e1";
        code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    /**
     * admin write data test
     */
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX t1(t_c) VALUES 1:(1)";
        auto code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "INSERT EDGE e1(e_c) VALUES 1 -> 2:(95)";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "GO FROM 1 OVER e1";
        code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    /**
     * dba write data test
     */
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX t1(t_c) VALUES 1:(1)";
        auto code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "INSERT EDGE e1(e_c) VALUES 1 -> 2:(95)";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "GO FROM 1 OVER e1";
        code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    /**
     * user write data test
     */
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX t1(t_c) VALUES 1:(1)";
        auto code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "INSERT EDGE e1(e_c) VALUES 1 -> 2:(95)";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        query = "GO FROM 1 OVER e1";
        code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    /**
     * guest write data test
     */
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX t1(t_c) VALUES 1:(1)";
        auto code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "INSERT EDGE e1(e_c) VALUES 1 -> 2:(95)";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);

        query = "GO FROM 1 OVER e1";
        code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    /**
     * use space test
     */
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space3(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space3";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space3";
        auto code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space3";
        auto code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space3";
        auto code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space3";
        auto code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
}
TEST_F(PermissionTest, ShowTest) {
    auto client = gEnv->getClient();
    auto adminClient = gEnv->getClient("admin", "admin");
    auto dbaClient = gEnv->getClient("dba", "dba");
    auto userClient = gEnv->getClient("user", "user");
    auto guestClient = gEnv->getClient("guest", "guest");
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space4(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW SPACES";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(5, resp.get_rows()->size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW SPACES";
        auto code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(1, resp.get_rows()->size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW SPACES";
        auto code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(1, resp.get_rows()->size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW SPACES";
        auto code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(1, resp.get_rows()->size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW SPACES";
        auto code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(1, resp.get_rows()->size());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW ROLES IN space1";
        auto code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW ROLES IN space2";
        auto code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW ROLES IN space1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE SPACE space1";
        auto code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_BAD_PERMISSION, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE SPACE space1";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE SPACE space2";
        auto code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(PermissionTest, ShowRolesTest) {
    auto client = gEnv->getClient();
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space5(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE ADMIN ON space5 TO admin";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE DBA ON space5 TO dba";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE USER ON space5 TO user";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE GUEST ON space5 TO guest";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    auto adminClient = gEnv->getClient("admin", "admin");
    auto dbaClient = gEnv->getClient("dba", "dba");
    auto userClient = gEnv->getClient("user", "user");
    auto guestClient = gEnv->getClient("guest", "guest");
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW ROLES IN space5";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(4, resp.get_rows()->size());
        std::vector<std::tuple<std::string, std::string>> rows = {
            {"guest", "GUEST"},
            {"user", "USER"},
            {"dba", "DBA"},
            {"admin", "ADMIN"}
        };
        ASSERT_TRUE(verifyResult(resp, rows));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW ROLES IN space5";
        auto code = adminClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(4, resp.get_rows()->size());
        std::vector<std::tuple<std::string, std::string>> rows = {
            {"guest", "GUEST"},
            {"user", "USER"},
            {"dba", "DBA"},
            {"admin", "ADMIN"}
        };
        ASSERT_TRUE(verifyResult(resp, rows));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW ROLES IN space5";
        auto code = dbaClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(1, resp.get_rows()->size());
        std::vector<std::tuple<std::string, std::string>> rows = {
            {"dba", "DBA"}
        };
        ASSERT_TRUE(verifyResult(resp, rows));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW ROLES IN space5";
        auto code = userClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(1, resp.get_rows()->size());
        std::vector<std::tuple<std::string, std::string>> rows = {
            {"user", "USER"}
        };
        ASSERT_TRUE(verifyResult(resp, rows));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW ROLES IN space5";
        auto code = guestClient->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_EQ(1, resp.get_rows()->size());
        std::vector<std::tuple<std::string, std::string>> rows = {
            {"guest", "GUEST"}
        };
        ASSERT_TRUE(verifyResult(resp, rows));
    }
}
}   // namespace graph
}   // namespace nebula
