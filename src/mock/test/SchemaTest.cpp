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
class SchemaTest : public TestBase {
public:
    void SetUp() override {
        TestBase::SetUp();
    };

    void TearDown() override {
        TestBase::TearDown();
    };

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

std::unique_ptr<GraphClient> SchemaTest::client_{nullptr};

TEST_F(SchemaTest, TestSpace) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_for_default(partition_num=9, replica_factor=1);";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC SPACE space_for_default;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        DataSet expect;
        expect.colNames = {"ID", "Name", "Partition number", "Replica Factor",
                           "Vid Size", "Charset", "Collate"};
        std::vector<Row> rows;
        std::vector<Value> columns;
        Row row;
        columns.emplace_back(1);
        columns.emplace_back(9);
        columns.emplace_back(1);
        columns.emplace_back(8);
        columns.emplace_back("");
        columns.emplace_back("");
        row.columns = std::move(columns);
        rows.emplace_back(row);
        expect.rows = rows;
        ASSERT_TRUE(resp.__isset.data);
        ASSERT_EQ(1, resp.get_data()->size());
        // ASSERT_EQ(expect, (*resp.get_data())[0]);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space_for_default";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(SchemaTest, TestTag) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG student(name STRING, age INT8, grade FIXED_STRING(10));";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC TAG student;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(resp.__isset.data);
        ASSERT_EQ(1, resp.get_data()->size());
    }
}

TEST_F(SchemaTest, TestEdge) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE schoolmate(start int, end int);";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC EDGE schoolmate;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(resp.__isset.data);
        ASSERT_EQ(1, resp.get_data()->size());
    }
}

TEST_F(SchemaTest, TestInsert) {
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT VERTEX student(name, age, grade) "
                            "VALUES \"Tom\":(\"Tom\", 18, \"three\");";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT VERTEX student(name, age, grade) "
                            "VALUES \"Lily\":(\"Lily\", 18, \"three\");";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT VERTEX student(name, age, grade) "
                            "VALUES 100:(\"100\", 17, \"three\");";
        auto code = client_->execute(query, resp);
        ASSERT_NE(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT EDGE schoolmate(start, end) "
                            "VALUES \"Tom\"->\"Lily\":(2009, 2011)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}
}   // namespace graph
}   // namespace nebula
