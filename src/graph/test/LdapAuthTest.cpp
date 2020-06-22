/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "graph/test/TestEnv.h"
#include "graph/test/TestBase.h"
#include "graph/GraphFlags.h"
#include "graph/Authenticator.h"
#include "graph/LdapAuthenticator.h"
#include "meta/test/TestUtils.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class LdapAuthTest : public TestBase {
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

TEST_F(LdapAuthTest, SimpleBindAuth) {
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_enable_authorize = true;
    FLAGS_auth_type = "password";
    {
        auto client = gEnv->getClient();
        ASSERT_NE(nullptr, client);

        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space1(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        sleep(FLAGS_heartbeat_interval_secs + 1);

        // Create shadow account
        cpp2::ExecutionResponse resp1;
        std::string query1 = "CREATE USER test2 WITH PASSWORD \"\"";
        auto code1 = client->execute(query1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string query2 = "GRANT ROLE ADMIN ON space1 TO test2";
        auto code2 = client->execute(query2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        // exist auth_ldap.so in share directory
        cpp2::ExecutionResponse resp3;
        std::string query3 = "install plugin auth_ldap soname \"auth_ldap.so\"";
        auto code3 = client->execute(query3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);

    FLAGS_auth_type = "ldap";
    FLAGS_ldap_server = "127.0.0.1";
    FLAGS_ldap_port = 389;
    FLAGS_ldap_scheme = "ldap";

    FLAGS_ldap_prefix = "uid=";
    FLAGS_ldap_suffix = ",ou=it,dc=sys,dc=com";

    // Ldap server contains corresponding records
    {
        auto adminClient = gEnv->getClient("test2", "passwdtest2");
        ASSERT_NE(nullptr, adminClient);
    }
    // Ldap server has no corresponding record
    {
        auto adminClient = gEnv->getClient("admin", "987");
        ASSERT_EQ(nullptr, adminClient);
    }
    {
        auto adminClient = gEnv->getClient("panda", "123456");
        ASSERT_EQ(nullptr, adminClient);
    }

    FLAGS_auth_type = "password";
    {
        auto client = gEnv->getClient();
        ASSERT_NE(nullptr, client);

        // exist auth_ldap.so in share directory
        cpp2::ExecutionResponse resp1;
        std::string query1 = "uninstall plugin auth_ldap";
        auto code1 = client->execute(query1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string query2 = "DROP USER test2";
        auto code2 = client->execute(query2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);
    }
}

TEST_F(LdapAuthTest, SearchBindAuth) {
    FLAGS_heartbeat_interval_secs = 1;
    FLAGS_enable_authorize = true;
    {
        auto client = gEnv->getClient();
        ASSERT_NE(nullptr, client);

        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space1(partition_num=1, replica_factor=1)";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        sleep(FLAGS_heartbeat_interval_secs + 1);

        // Create shadow account
        cpp2::ExecutionResponse resp1;
        std::string query1 = "CREATE USER test2 WITH PASSWORD \"\"";
        auto code1 = client->execute(query1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string query2 = "GRANT ROLE ADMIN ON space1 TO test2";
        auto code2 = client->execute(query2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);

        // exist auth_ldap.so in share directory
        cpp2::ExecutionResponse resp3;
        std::string query3 = "install plugin auth_ldap soname \"auth_ldap.so\"";
        auto code3 = client->execute(query3, resp3);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code3);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);

    FLAGS_auth_type = "ldap";
    FLAGS_ldap_server = "127.0.0.1";
    FLAGS_ldap_port = 389;
    FLAGS_ldap_scheme = "ldap";

    FLAGS_ldap_prefix = "";
    FLAGS_ldap_suffix = "";

    FLAGS_ldap_basedn = "uid=test2,ou=it,dc=sys,dc=com";
    // Ldap server contains corresponding records
    {
        auto adminClient = gEnv->getClient("test2", "passwdtest2");
        ASSERT_NE(nullptr, adminClient);
    }
    // Ldap server has no corresponding record
    {
        auto adminClient = gEnv->getClient("admin", "987");
        ASSERT_EQ(nullptr, adminClient);
    }
    {
        auto adminClient = gEnv->getClient("panda", "123456");
        ASSERT_EQ(nullptr, adminClient);
    }

    FLAGS_auth_type = "password";
    {
        auto client = gEnv->getClient();
        ASSERT_NE(nullptr, client);

        // exist auth_ldap.so in share directory
        cpp2::ExecutionResponse resp1;
        std::string query1 = "uninstall plugin auth_ldap";
        auto code1 = client->execute(query1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string query2 = "DROP USER test2";
        auto code2 = client->execute(query2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);
    }
}

}   // namespace graph
}   // namespace nebula
