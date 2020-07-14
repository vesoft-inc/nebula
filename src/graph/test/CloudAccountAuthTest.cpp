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
        std::string query1 = "CREATE USER admin WITH PASSWORD \"\"";
        auto code1 = client->execute(query1, resp1);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code1);

        cpp2::ExecutionResponse resp2;
        std::string query2 = "GRANT ROLE ADMIN ON space1 TO admin";
        auto code2 = client->execute(query2, resp2);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code2);
    }

    sleep(FLAGS_heartbeat_interval_secs + 1);
    FLAGS_auth_type = "cloud";
    FLAGS_cloud_http_url = "http://192.168.8.51:3000/mock/12/api/account/login";
    {
        auto adminClient = gEnv->getClient("admin", "admin");
        ASSERT_NE(nullptr, adminClient);
    }
    {
        auto adminClient = gEnv->getClient("panda", "admin");
        ASSERT_EQ(nullptr, adminClient);
    }

    FLAGS_auth_type = "password";
    {
        auto client = gEnv->getClient();
        ASSERT_NE(nullptr, client);
        cpp2::ExecutionResponse resp;
        std::string query = "DROP USER admin";
        auto code = client->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}
}   // namespace graph
}   // namespace nebula
