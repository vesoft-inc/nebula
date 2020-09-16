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
        std::string query = "CREATE SPACE space_set_vid_type(partition_num=9, "
                            "replica_factor=1, vid_type=FIXED_STRING(20));";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC SPACE space_for_default;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"ID", "Name", "Partition Number", "Replica Factor",
                                             "Charset", "Collate", "Vid Type"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<Value> values = {1, "space_for_default", 9,
                                     1, "utf8", "utf8_bin", "FIXED_STRING(8)"};
        ASSERT_TRUE(verifyValues(resp, values));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE SPACE space_2("
                            "partition_num=9, replica_factor=1, vid_type=FIXED_STRING(20));";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW SPACES;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"Name"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {
            {"space_2"},
            {"space_for_default"},
            {"space_set_vid_type"}
        };
        ASSERT_TRUE(verifyValues(resp, values));
    }
    // Show Create space
    std::string createSpaceStr;
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE SPACE space_2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        createSpaceStr = "CREATE SPACE `space_2` ("
                         "partition_num = 9, "
                         "replica_factor = 1, "
                         "charset = utf8, "
                         "collate = utf8_bin, "
                         "vid_type = FIXED_STRING(20))";
        std::vector<std::string> colNames = {"Space", "Create Space"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<Value> values = {"space_2", createSpaceStr};
        ASSERT_TRUE(verifyValues(resp, values));
    }
    // Drop space
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP SPACE space_2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW SPACES;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"Name"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {{"space_for_default"}, {"space_set_vid_type"}};
        ASSERT_TRUE(verifyValues(resp, values));
    }
    // use show create space result
    {
        cpp2::ExecutionResponse resp;
        auto code = client_->execute(createSpaceStr, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::string query = "SHOW SPACES;";
        code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"Name"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {
            {"space_2"},
            {"space_for_default"},
            {"space_set_vid_type"}
        };
        ASSERT_TRUE(verifyValues(resp, values));
    }
}

TEST_F(SchemaTest, TestTag) {
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        std::string query = "USE space_for_default";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG student(name STRING NOT NULL, "
                            "age INT8 DEFAULT 18, grade FIXED_STRING(10), start INT64 DEFAULT 2020)"
                            " ttl_duration = 3600, ttl_col = \"start\";";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC TAG student;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(resp.__isset.data);
        std::vector<std::string> colNames = {"Field", "Type", "Null", "Default"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {
                {"name", "string", "NO",  Value::kEmpty},
                {"age", "int8", "YES", 18},
                {"grade", "fixed_string(10)", "YES", Value::kEmpty},
                {"start", "int64", "YES", 2020}};
        ASSERT_TRUE(verifyValues(resp, values));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE TAG person(name STRING DEFAULT \"\", amount FLOAT);";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW TAGS;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"Name"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {{"person"}, {"student"}};
        ASSERT_TRUE(verifyValues(resp, values));
    }
    // Show Create tag
    std::string createTagStr;
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE TAG student";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        createTagStr = "CREATE TAG `student` (\n"
                        " `name` string NOT NULL,\n"
                        " `age` int8 NULL DEFAULT 18,\n"
                        " `grade` fixed_string(10) NULL,\n"
                        " `start` int64 NULL DEFAULT 2020\n"
                        ") ttl_duration = 3600, ttl_col = \"start\"";
        std::vector<std::string> colNames = {"Tag", "Create Tag"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<Value> values = {"student", createTagStr};
        ASSERT_TRUE(verifyValues(resp, values));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP TAG student;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW TAGS;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"Name"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<Value> values = {"person"};
        ASSERT_TRUE(verifyValues(resp, values));
    }
    // Check the show create tag result is ok
    {
        cpp2::ExecutionResponse resp;
        auto code = client_->execute(createTagStr, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::string query = "SHOW TAGS;";
        code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"Name"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {{"person"}, {"student"}};
        ASSERT_TRUE(verifyValues(resp, values));
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
        std::string query = "USE space_for_default; "
                            "CREATE EDGE schoolmate(start int NOT NULL, end int);";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DESC EDGE schoolmate;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(resp.__isset.data);
        std::vector<std::string> colNames = {"Field", "Type", "Null", "Default"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {
                {"start", "int64", "NO", Value::kEmpty},
                {"end", "int64", "YES", Value::kEmpty}};
        ASSERT_TRUE(verifyValues(resp, values));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "CREATE EDGE like(name STRING NOT NULL DEFAULT \"\", amount FLOAT);";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW EDGES;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"Name"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {{"like"}, {"schoolmate"}};
        ASSERT_TRUE(verifyValues(resp, values));
    }
    // Show Create edge
    std::string createEdgeStr;
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW CREATE EDGE like";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        createEdgeStr = "CREATE EDGE `like` (\n"
                        " `name` string NOT NULL DEFAULT \"\",\n"
                        " `amount` float NULL\n"
                        ") ttl_duration = 0, ttl_col = \"\"";
        std::vector<std::string> colNames = {"Edge", "Create Edge"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<Value> values = {"like", createEdgeStr};
        ASSERT_TRUE(verifyValues(resp, values));
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "DROP EDGE like;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        std::string query = "SHOW EDGES;";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"Name"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<Value> values = {"schoolmate"};
        ASSERT_TRUE(verifyValues(resp, values));
    }
    // Check the show create edge result is ok
    {
        cpp2::ExecutionResponse resp;
        auto code = client_->execute(createEdgeStr, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);

        std::string query = "SHOW EDGES;";
        code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::string> colNames = {"Name"};
        ASSERT_TRUE(verifyColNames(resp, colNames));
        std::vector<std::vector<Value>> values = {{"like"}, {"schoolmate"}};
        ASSERT_TRUE(verifyValues(resp, values));
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
