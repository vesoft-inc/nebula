/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Status.h"
#include "common/network/NetworkUtils.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "mock/test/TestEnv.h"
#include "mock/test/TestBase.h"
#include <gtest/gtest.h>

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class ACLTest : public TestBase {
public:
    void SetUp() override {
        TestBase::SetUp();
        client_ = gEnv->getGraphClient();
        ASSERT_NE(nullptr, client_);
    };

    void TearDown() override {
        TestBase::TearDown();
        client_.reset();
    };

protected:
    std::unique_ptr<GraphClient>     client_;
};

TEST_F(ACLTest, Error) {
    {
        // alter user not exists
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER USER not_exists WITH PASSWORD \"not_exists\";";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
    {
        // drop user not exists
        cpp2::ExecutionResponse resp;
        std::string query = "DROP USER not_exists;";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
    {
        // grant to user not exists
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE ADMIN ON not_exists TO not_exists";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
    {
        // revoke from user not exists
        cpp2::ExecutionResponse resp;
        std::string query = "REVOKE ROLE ADMIN ON not_exists FROM not_exists";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
}

TEST_F(ACLTest, Simple) {
    {
        // drop user if exists
        cpp2::ExecutionResponse resp;
        std::string query = "DROP USER IF EXISTS not_exists;";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // create user
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER test1";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // create user with password
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER test2 WITH PASSWORD \"123456\"";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // create user exists
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE USER IF NOT EXISTS test2 WITH PASSWORD \"123456\"";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // create one space
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE test";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
        sleep(FLAGS_heartbeat_interval_secs + 1);
    }
    {
        // grant
        cpp2::ExecutionResponse resp;
        std::string query = "GRANT ROLE ADMIN ON test TO test1";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // revoke
        cpp2::ExecutionResponse resp;
        std::string query = "REVOKE ROLE ADMIN ON test FROM test1";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // drop user
        cpp2::ExecutionResponse resp;
        std::string query = "DROP USER test2";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
}

}   // namespace graph
}   // namespace nebula
