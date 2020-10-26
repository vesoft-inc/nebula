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

class AdminJobTest : public TestBase {
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

TEST_F(AdminJobTest, Error) {
    {
        // submit without space
        cpp2::ExecutionResponse resp;
        std::string query = "SUBMIT JOB COMPACT";
        client_->execute(query, resp);
        // TODO(shylock) semantic error?
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_SEMANTIC_ERROR);
    }
    {
        // show one not exists
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW JOB 233";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
    {
        // stop one not exists
        cpp2::ExecutionResponse resp;
        std::string query = "STOP JOB 233";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
}

TEST_F(AdminJobTest, Base) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_for_default(partition_num=9, replica_factor=1);";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space_for_default;";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // submit
        cpp2::ExecutionResponse resp;
        std::string query = "SUBMIT JOB COMPACT";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        DataSet expected({"New Job Id"});
        expected.emplace_back(Row({
            2
        }));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
    {
        // submit
        cpp2::ExecutionResponse resp;
        std::string query = "SUBMIT JOB FLUSH";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        DataSet expected({"New Job Id"});
        expected.emplace_back(Row({
            3
        }));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
    {
        // show all
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW JOBS";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        nebula::DataSet expected({"Job Id", "Command", "Status", "Start Time", "Stop Time"});
        expected.emplace_back(Row({
            2, "COMPACT", "QUEUE", 0, 0,
        }));
        expected.emplace_back(Row({
            3, "FLUSH", "QUEUE", 0, 0
        }));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
    // TODO current not support should move it into show
    /*
    {
        // show one
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW JOB 2";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        nebula::DataSet expected(
            {"Job Id(TaskId)", "Command(Dest)", "Status", "Start Time", "Stop Time"});
        expected.emplace_back(Row({2, "COMPACT", "QUEUE", 0, 0}));
        expected.emplace_back(Row({1, "127.0.0.1", "QUEUE", 0, 0}));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
    {
        // stop one
        cpp2::ExecutionResponse resp;
        std::string query = "STOP JOB 2";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        nebula::DataSet expected({"Result"});
        expected.emplace_back(Row({"Job stopped"}));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
    {
        // show all
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW JOBS";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        nebula::DataSet expected({"Job Id", "Command", "Status", "Start Time", "Stop Time"});
        expected.emplace_back(Row({
            2, "COMPACT", "STOPPED", 0, 0,
        }));
        expected.emplace_back(Row({
            3, "FLUSH", "QUEUE", 0, 0
        }));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
    {
        // recover
        cpp2::ExecutionResponse resp;
        std::string query = "RECOVER JOB";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        nebula::DataSet expected({"Recovered job num"});
        expected.emplace_back(Row({
            0
        }));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
    */
}

}   // namespace graph
}   // namespace nebula
