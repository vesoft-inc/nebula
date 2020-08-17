/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Status.h"
#include "common/interface/gen-cpp2/common_types.h"
#include "common/network/NetworkUtils.h"
#include "mock/test/TestBase.h"
#include "mock/test/TestEnv.h"

DECLARE_int32(heartbeat_interval_secs);

namespace nebula {
namespace graph {

class BalanceTest : public TestBase {
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
    std::unique_ptr<GraphClient> client_;
};

TEST_F(BalanceTest, Error) {
    {
        // Show one not exists
        cpp2::ExecutionResponse resp;
        std::string query = "BALANCE DATA 233;";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
    {
        // stop not exists
        cpp2::ExecutionResponse resp;
        std::string query = "BALANCE DATA STOP;";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::E_EXECUTION_ERROR);
    }
}

TEST_F(BalanceTest, Simple) {
    {
        // balance leader
        cpp2::ExecutionResponse resp;
        std::string query = "BALANCE LEADER;";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // balance without space
        cpp2::ExecutionResponse resp;
        std::string query = "BALANCE DATA;";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        DataSet expected({"ID"});
        expected.emplace_back(Row({1}));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
    {
        // show one without space
        cpp2::ExecutionResponse resp;
        std::string query = "BALANCE DATA 1;";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        DataSet expected({"balanceId, spaceId:partId, src->dst", "status"});
        expected.emplace_back(Row({
            "Total:0, Succeeded:0, Failed:0, In Progress:0, Invalid:0",
            0,
        }));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
    {
        // stop
        cpp2::ExecutionResponse resp;
        std::string query = "BALANCE DATA STOP;";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // create space
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space1(partition_num=3, replica_factor=1);";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        // create space
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space2(partition_num=9, replica_factor=3);";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "BALANCE DATA;";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        DataSet expected({"ID"});
        expected.emplace_back(Row({4}));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
    {
        // show one
        cpp2::ExecutionResponse resp;
        std::string query = "BALANCE DATA 4;";
        client_->execute(query, resp);
        ASSERT_ERROR_CODE(resp, cpp2::ErrorCode::SUCCEEDED);

        // TODO(shylock) dataset constructor
        auto storagePort = gEnv->storageServerPort();
        DataSet expected({"balanceId, spaceId:partId, src->dst", "status"});
        expected.emplace_back(Row({
            folly::sformat("[4, 3:1, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(Row({
            folly::sformat("[4, 3:2, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(Row({
            folly::sformat("[4, 3:3, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(Row({
            folly::sformat("[4, 3:4, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(Row({
            folly::sformat("[4, 3:5, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(Row({
            folly::sformat("[4, 3:6, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(Row({
            folly::sformat("[4, 3:7, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(Row({
            folly::sformat("[4, 3:8, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(Row({
            folly::sformat("[4, 3:9, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(Row({
            folly::sformat("[4, 2:1, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(Row({
            folly::sformat("[4, 2:2, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(Row({
            folly::sformat("[4, 2:3, [127.0.0.1:{}]->[127.0.0.1:{}]]", storagePort, storagePort),
            "IN_PROGRESS",
        }));
        expected.emplace_back(
            Row({"Total:12, Succeeded:0, Failed:0, In Progress:12, Invalid:0", 0}));
        ASSERT_TRUE(verifyDataSetWithoutOrder(resp, expected));
    }
}

}   // namespace graph
}   // namespace nebula
