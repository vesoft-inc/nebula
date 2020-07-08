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
        std::string query = "CREATE SPACE space_set_vid_size(partition_num=9, "
                            "replica_factor=1, vid_size=20);";
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
        std::vector<Value> columns = {Value(1), Value("space_for_default"), Value(9),
                                      Value(1), Value(8), Value("utf8"), Value("utf8_bin")};
        Row row;
        row.values = std::move(columns);
        rows.emplace_back(row);
        expect.rows = rows;
        ASSERT_TRUE(resp.__isset.data);
        ASSERT_EQ(expect, *resp.get_data());
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
        DataSet expect;
        expect.colNames = {"Field", "Type", "Null", "Default"};
        std::vector<Row> rows;
        std::vector<Value> columns1 = {Value("name"), Value("string"), Value("YES"), Value()};
        std::vector<Value> columns2 = {Value("age"), Value("int8"), Value("YES"), Value()};
        std::vector<Value> columns3 = {Value("grade"), Value("fixed_string(10)"),
                                       Value("YES"), Value()};
        Row row;
        row.values = std::move(columns1);
        rows.emplace_back(row);
        row.values = std::move(columns2);
        rows.emplace_back(row);
        row.values = std::move(columns3);
        rows.emplace_back(row);
        expect.rows = std::move(rows);
        ASSERT_TRUE(resp.__isset.data);
        ASSERT_EQ(expect, *resp.get_data());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE A; USE A; "
                            "CREATE TAG tag1(name STRING, age INT8, grade FIXED_STRING(10));";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(SchemaTest, TestEdge) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space_for_default; CREATE EDGE schoolmate(start int, end int);";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC EDGE schoolmate;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(resp.__isset.data);
        DataSet expect;
        expect.colNames = {"Field", "Type", "Null", "Default"};
        std::vector<Row> rows;
        std::vector<Value> columns1 = {Value("start"), Value("int64"), Value("YES"), Value()};
        std::vector<Value> columns2 = {Value("end"), Value("int64"), Value("YES"), Value()};
        Row row;
        row.values = std::move(columns1);
        rows.emplace_back(row);
        row.values = std::move(columns2);
        rows.emplace_back(row);
        expect.rows = std::move(rows);
        ASSERT_EQ(expect, *resp.get_data());
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE B; USE B; "
                            "CREATE EDGE edge1(name STRING, age INT8, grade FIXED_STRING(10));";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}

TEST_F(SchemaTest, TestAlterTag) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG alterTag(col1 STRING, col2 INT8, "
                            "col3 DOUBLE, col4 FIXED_STRING(10));";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER TAG alterTag "
                            "ADD (col5 TIMESTAMP, col6 DATE NOT NULL), "
                            "CHANGE (col2 INT8 DEFAULT 10), "
                            "DROP (col4)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC TAG alterTag;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(resp.__isset.data);
        std::vector<std::string> colNames = {"Field", "Type", "Null", "Default"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {
                {Value("col1"), Value("string"), Value("YES"), Value()},
                {Value("col2"), Value("int8"), Value("YES"), Value(10)},
                {Value("col3"), Value("double"), Value("YES"), Value()},
                {Value("col5"), Value("timestamp"), Value("YES"), Value()},
                {Value("col6"), Value("date"), Value("NO"), Value()}};
        ASSERT_TRUE(verifyValues(resp, values));
    }
}

TEST_F(SchemaTest, TestAlterEdge) {
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE alterEdge(col1 STRING, col2 INT8, "
                            "col3 DOUBLE, col4 FIXED_STRING(10));";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        }
        {
        cpp2::ExecutionResponse resp;
        std::string query = "ALTER EDGE alterEdge "
                            "ADD (col5 TIMESTAMP, col6 DATE NOT NULL), "
                            "CHANGE (col2 INT8 DEFAULT 10), "
                            "DROP (col4)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC EDGE alterEdge;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(resp.__isset.data);
        std::vector<std::string> colNames = {"Field", "Type", "Null", "Default"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {
                {Value("col1"), Value("string"), Value("YES"), Value()},
                {Value("col2"), Value("int8"), Value("YES"), Value(10)},
                {Value("col3"), Value("double"), Value("YES"), Value()},
                {Value("col5"), Value("timestamp"), Value("YES"), Value()},
                {Value("col6"), Value("date"), Value("NO"), Value()}};
        ASSERT_TRUE(verifyValues(resp, values));
    }
}

TEST_F(SchemaTest, TestInsert) {
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space_for_default;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space_for_default; INSERT VERTEX student(name, age, grade) "
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
                            "VALUES \"Tom\":(\"Tom\", 18, \"three\");"
                            "INSERT VERTEX student(name, age, grade) "
                            "VALUES \"Tom\":(\"Tom\", 18, \"three\");";
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
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT EDGE schoolmate(start, end) "
                            "VALUES \"Tom\"->\"Lily\":(2009, 2011)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "INSERT EDGE schoolmate(start, end) "
                            "VALUES \"Tom\"->\"Lily\":(2009, 2011);"
                            "INSERT EDGE schoolmate(start, end) "
                            "VALUES \"Tom\"->\"Lily\":(2009, 2011)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
}
}   // namespace graph
}   // namespace nebula
