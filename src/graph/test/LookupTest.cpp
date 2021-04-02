/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/test/LookupTestBase.h"
#include "graph/LookupExecutor.h"

namespace nebula {
namespace graph {

class LookupTest : public LookupTestBase {
protected:
    void SetUp() override {
        LookupTestBase::SetUp();
        // ...
    }

    void TearDown() override {
        // ...
        LookupTestBase::TearDown();
    }
};

TEST_F(LookupTest, SimpleVertex) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX lookup_tag_1(col1, col2, col3) VALUES "
                     "200:(200, 200, 200), "
                     "201:(201, 201, 201), "
                     "202:(202, 202, 202)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        /**
         * kPrimary == kPrimary
         */
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE col1 == 200";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == 300";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == 200";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {200},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"VertexID"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == 200 "
                     "YIELD lookup_tag_1.col1, lookup_tag_1.col2, lookup_tag_1.col3";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, int64_t, int64_t, int64_t>> expected = {
            {200, 200, 200, 200},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"VertexID"},
            {"lookup_tag_1.col1"},
            {"lookup_tag_1.col2"},
            {"lookup_tag_1.col3"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
}

TEST_F(LookupTest, SimpleEdge) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE lookup_edge_1(col1, col2, col3) VALUES "
                     "200 -> 201@0:(201, 201, 201), "
                     "200 -> 202@0:(202, 202, 202)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        /**
         * kPrimary == kPrimary
         */
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE col1 == 201";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 300";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 201";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {200, 201, 0},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"SrcVID", "DstVID", "Ranking"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 201 "
                     "YIELD lookup_edge_1.col1, lookup_edge_1.col2, lookup_edge_1.col3";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking,
            int64_t, int64_t, int64_t>> expected = {
            {200, 201, 0, 201, 201, 201},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"SrcVID"},
            {"DstVID"},
            {"Ranking"},
            {"lookup_edge_1.col1"},
            {"lookup_edge_1.col2"},
            {"lookup_edge_1.col3"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
}

TEST_F(LookupTest, VertexIndexHint) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX lookup_tag_1(col1, col2, col3) VALUES "
                     "200:(200, 200, 200), "
                     "201:(201, 201, 201), "
                     "202:(202, 202, 202)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col2 == 200";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {200},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    /**
     * No matching index was found
     */
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col1 == true";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(LookupTest, EdgeIndexHint) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE lookup_edge_1(col1, col2, col3) VALUES "
                     "200 -> 201@0:(201, 201, 201), "
                     "200 -> 202@0:(202, 202, 202)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col2 == 201";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {200, 201, 0},
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    /**
     * No matching index was found
     */
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col1 == 200";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(LookupTest, VertexConditionScan) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX lookup_tag_2(col1, col2, col3, col4) VALUES "
                     "220:(true, 100, 100.5, true), "
                     "221:(true, 200, 200.5, true), "
                     "222:(true, 300, 300.5, true), "
                     "223:(true, 400, 400.5, true), "
                     "224:(true, 500, 500.5, true), "
                     "225:(true, 600, 600.5, true)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 == 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {220}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 == 100 "
                     "OR lookup_tag_2.col2 == 200";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 > 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {221, 222, 223, 224, 225}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 != 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {221, 222, 223, 224, 225}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 >= 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {220, 221, 222, 223, 224, 225}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 >= 100 "
                     "AND lookup_tag_2.col4 == true";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {220, 221, 222, 223, 224, 225}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 >= 100 "
                     "AND lookup_tag_2.col4 != true";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_FALSE(resp.__isset.rows);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 >= 100 "
                     "AND lookup_tag_2.col2 <= 400";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {220, 221, 222, 223}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 == 100.5 "
                     "AND lookup_tag_2.col2 == 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {220}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 == 100.5 "
                     "AND lookup_tag_2.col2 == 200";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 > 100.5";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {221, 222, 223, 224, 225}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 == 100.5";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {220}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 == 100.1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col3 >= 100.5 "
                     "AND lookup_tag_2.col3 <= 300.5";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {220, 221, 222}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(LookupTest, EdgeConditionScan) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX lookup_tag_2(col1, col2, col3, col4) VALUES "
                     "220:(true, 100, 100.5, true), "
                     "221:(true, 200, 200.5, true), "
                     "222:(true, 300, 300.5, true), "
                     "223:(true, 400, 400.5, true), "
                     "224:(true, 500, 500.5, true), "
                     "225:(true, 600, 600.5, true)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE lookup_edge_2(col1, col2, col3, col4) VALUES "
                     "220 -> 221@0:(true, 100, 100.5, true), "
                     "220 -> 222@0:(true, 200, 200.5, true), "
                     "220 -> 223@0:(true, 300, 300.5, true), "
                     "220 -> 224@0:(true, 400, 400.5, true), "
                     "220 -> 225@0:(true, 500, 500.5, true)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 == 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {220, 221, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 == 100 "
                     "OR lookup_edge_2.col2 == 200";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 > 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {220, 222, 0},
            {220, 223, 0},
            {220, 224, 0},
            {220, 225, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 != 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {220, 222, 0},
            {220, 223, 0},
            {220, 224, 0},
            {220, 225, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 >= 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {220, 221, 0},
            {220, 222, 0},
            {220, 223, 0},
            {220, 224, 0},
            {220, 225, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 >= 100 "
                     "AND lookup_edge_2.col4 == true";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {220, 221, 0},
            {220, 222, 0},
            {220, 223, 0},
            {220, 224, 0},
            {220, 225, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 >= 100 "
                     "AND lookup_edge_2.col4 != true";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_FALSE(resp.__isset.rows);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col2 >= 100 "
                     "AND lookup_edge_2.col2 <= 400";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {220, 221, 0},
            {220, 222, 0},
            {220, 223, 0},
            {220, 224, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 == 100.5 "
                     "AND lookup_edge_2.col2 == 100";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {220, 221, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 == 100.5 "
                     "AND lookup_edge_2.col2 == 200";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 > 100.5";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {220, 222, 0},
            {220, 223, 0},
            {220, 224, 0},
            {220, 225, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 == 100.5";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {220, 221, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 == 100.1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col3 >= 100.5 "
                     "AND lookup_edge_2.col3 <= 300.5";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {
            {220, 221, 0},
            {220, 222, 0},
            {220, 223, 0}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(LookupTest, FunctionExprTest) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX lookup_tag_2(col1, col2, col3, col4) VALUES "
                     "220:(true, 100, 100.5, true), "
                     "221:(true, 200, 200.5, true), "
                     "222:(true, 300, 300.5, true), "
                     "223:(true, 400, 400.5, true), "
                     "224:(true, 500, 500.5, true), "
                     "225:(true, 600, 600.5, true)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE 1 == 1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE 1 != 1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE udf_is_in(lookup_tag_2.col2, 100, 200)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 > abs(-5)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {220, 221, 222, 223, 224, 225}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 < abs(-5)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 > (1 + 2)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {220, 221, 222, 223, 224, 225}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 < (1 + 2)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col4 != (true && true)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col4 == (true && true)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {220, 221, 222, 223, 224, 225}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col4 == (true || false)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {
            {220, 221, 222, 223, 224, 225}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col4 == "
                     "strcasecmp(\"HelLo\", \"HelLo\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col4 == "
                     "strcasecmp(\"HelLo\", \"hello\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 != lookup_tag_2.col3";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col2 > (lookup_tag_2.col3 - 100)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(LookupTest, YieldClauseTest) {
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG student(number int, age int)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX student_index ON student(number, age)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG teacher(number int, age int)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX student(number, age), teacher(number, age)  VALUES "
                     "220:(1, 20, 1, 30), "
                     "221:(2, 22, 2, 32)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Invalid tag name in yield clause.
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON student WHERE student.number == 1 YIELD teacher.age";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Invalid tag name in yield clause. and Alias is same with tag name.
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON student WHERE student.number == 1 YIELD teacher.age"
                     " as student_name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Invalid tag name in where clause.
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON student WHERE teacher.number == 1 YIELD student.age";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON student WHERE student.number == 1 YIELD student.age";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, int64_t>> expected = {{220, 20}};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(LookupTest, OptimizerTest) {
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG t1(c1 int, c2 int, c3 int, c4 int, c5 int)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i1 ON t1(c1, c2)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i2 ON t1(c2, c1)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i3 ON t1(c3)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i4 ON t1(c1, c2, c3, c4)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i5 ON t1(c1, c2, c3, c5)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);

    auto mc = gEnv->metaClient();
    auto indexes = mc->getTagIndexesFromCache(1);
    ASSERT_TRUE(indexes.ok());
    auto ectx = std::make_unique<ExecutionContext>(nullptr,
                                                   gEnv->schemaManager(),
                                                   nullptr,
                                                   nullptr,
                                                   nullptr,
                                                   nullptr);
    auto executor = std::make_unique<LookupExecutor>(nullptr, ectx.get());
    std::map<std::string, IndexID> expected;
    {
        executor->indexes_ = indexes.value();
        executor->spaceId_ = 1;
        auto tagID = mc->getTagIDByNameFromCache(1, "t1");
        ASSERT_TRUE(tagID.ok());
        executor->tagOrEdge_ = tagID.value();
        for (int8_t i = 1; i <= 5; i++) {
            auto indexName = folly::stringPrintf("i%d", i);
            auto index = std::find_if(executor->indexes_.begin(), executor->indexes_.end(),
                                      [indexName](const auto &idx) {
                                          return idx->get_index_name() == indexName;
                                      });
            if (index != executor->indexes_.end()) {
                expected[index->get()->get_index_name()] = index->get()->get_index_id();
            }
        }
    }
    // tag (c1 , c2, c3, c4, c5)
    // i1 on tag (c1, c2)
    // i2 on tag (c2, c1)
    // i3 on tag (c3)
    // i4 on tag (c1, c2, c3, c4)
    // i5 on tag (c1, c2, c3, c5)
    {
        // "LOOKUP on t1 WHERE t1.c1 == 1"; expected i1 or i4
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::EQ);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i1"] == executor->index_ ||
                    expected["i4"] == executor->index_ ||
                    expected["i5"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c1 == 1 and c2 > 1"; expected i1 or i4 or i5
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::GT);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i1"] == executor->index_ ||
                    expected["i4"] == executor->index_ ||
                    expected["i5"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c1 > 1 and c2 == 1"; expected i2
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::GT);
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::EQ);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i2"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c1 > 1 and c2 == 1 and c3 == 1"; expected i4 or i5
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::GT);
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::EQ);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i4"] == executor->index_ || expected["i5"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c3 > 1"; expected i3
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::GT);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i3"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c3 > 1 and c1 > 1"; expected i4 or i5.
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::GT);
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::GT);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i4"] == executor->index_ || expected["i5"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c4 > 1"; No invalid index found.
        executor->filters_.emplace_back("c4", RelationalExpression::Operator::GT);
        ASSERT_FALSE(executor->findOptimalIndex().ok());
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c2 > 1 and c3 > 1"; No invalid index found.
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::GT);
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::GT);
        ASSERT_FALSE(executor->findOptimalIndex().ok());
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c2 > 1 and c1 != 1"; expected i2.
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::GT);
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::NE);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i2"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c2 != 1.
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::NE);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i2"] == executor->index_);
        executor->filters_.clear();
    }
    {
        for (int8_t i = 1; i <= 5; i++) {
            cpp2::ExecutionResponse resp;
            auto stmt = folly::stringPrintf("DROP TAG INDEX i%d", i);
            auto code = client_->execute(stmt, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        }
    }
}

TEST_F(LookupTest, OptimizerWithStringFieldTest) {
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG t1_str(c1 int, c2 int, c3 string, c4 string)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i1_str ON t1_str(c1, c2)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i2_str ON t1_str(c4)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i3_str ON t1_str(c3, c1)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i4_str ON t1_str(c3, c4)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i5_str ON t1_str(c1, c2, c3, c4)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);

    auto mc = gEnv->metaClient();
    auto indexes = mc->getTagIndexesFromCache(1);
    ASSERT_TRUE(indexes.ok());
    auto ectx = std::make_unique<ExecutionContext>(nullptr,
                                                   gEnv->schemaManager(),
                                                   nullptr,
                                                   nullptr,
                                                   nullptr,
                                                   nullptr);
    auto executor = std::make_unique<LookupExecutor>(nullptr, ectx.get());
    std::map<std::string, IndexID> expected;
    {
        executor->indexes_ = indexes.value();
        executor->spaceId_ = 1;
        auto tagID = mc->getTagIDByNameFromCache(1, "t1_str");
        ASSERT_TRUE(tagID.ok());
        executor->tagOrEdge_ = tagID.value();
        for (int8_t i = 1; i <= 5; i++) {
            auto indexName = folly::stringPrintf("i%d_str", i);
            auto index = std::find_if(executor->indexes_.begin(), executor->indexes_.end(),
                                      [indexName](const auto &idx) {
                                          return idx->get_index_name() == indexName;
                                      });
            if (index != executor->indexes_.end()) {
                expected[index->get()->get_index_name()] = index->get()->get_index_id();
            }
        }
    }
    {
        // "LOOKUP on t1_str WHERE c1 == 1"; expected i1_str
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::EQ);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        LOG(INFO) << "index is : " << executor->index_;
        ASSERT_TRUE(expected["i1_str"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1_str WHERE c1 == 1 and tc2 > 1"; expected i1_str
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::GT);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i1_str"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE c3 == "a". No invalid index found.
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::EQ);
        ASSERT_FALSE(executor->findOptimalIndex().ok());
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE c4 == "a". expected i2_str
        executor->filters_.emplace_back("c4", RelationalExpression::Operator::EQ);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i2_str"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE c3 == "a" and c4 == "a". expected i4_str
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c4", RelationalExpression::Operator::EQ);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i4_str"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE c3 == "a" and c1 == 1. expected i3_str
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::EQ);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i3_str"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE c3 == "a" and c2 == 1 and c1 == 1. No invalid index found.
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::EQ);
        ASSERT_FALSE(executor->findOptimalIndex().ok());
    }
    {
        // "LOOKUP on t1 WHERE
        // c4 == "a" and c3 == "a" and c2 == 1 and c1 == 1. No invalid index found.
        executor->filters_.emplace_back("c4", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::EQ);
        ASSERT_TRUE(executor->findOptimalIndex().ok());
        ASSERT_TRUE(expected["i5_str"] == executor->index_);
        executor->filters_.clear();
    }
    {
        for (int8_t i = 1; i <= 5; i++) {
            cpp2::ExecutionResponse resp;
            auto stmt = folly::stringPrintf("DROP TAG INDEX i%d_str", i);
            auto code = client_->execute(stmt, resp);
            ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        }
    }
}

TEST_F(LookupTest, StringFieldTest) {
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG tag_with_str(c1 int, c2 string, c3 string)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i1_with_str ON tag_with_str(c1, c2)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i2_with_str ON tag_with_str(c2, c3)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX i3_with_str ON tag_with_str(c1, c2, c3)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX tag_with_str(c1, c2, c3) VALUES "
                     "1:(1, \"c1_row1\", \"c2_row1\"), "
                     "2:(2, \"c1_row2\", \"c2_row2\"), "
                     "3:(3, \"abc\", \"abc\"), "
                     "4:(4, \"abc\", \"abc\"), "
                     "5:(5, \"ab\", \"cabc\"), "
                     "6:(5, \"abca\", \"bc\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // No valid index found, the where condition need refer to all index fields.
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON tag_with_str WHERE tag_with_str.c1 == 1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON tag_with_str WHERE "
                     "tag_with_str.c1 == 1 and tag_with_str.c2 == \"ccc\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(!resp.__isset.rows || resp.get_rows()->size() == 0);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON tag_with_str WHERE "
                     "tag_with_str.c1 == 1 and tag_with_str.c2 == \"c1_row1\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {{1}};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON tag_with_str WHERE "
                     "tag_with_str.c1 == 5 and tag_with_str.c2 == \"ab\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {{5}};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON tag_with_str WHERE "
                     "tag_with_str.c2 == \"abc\" and tag_with_str.c3 == \"abc\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {{3}, {4}};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON tag_with_str WHERE "
                     "tag_with_str.c1 == 5 and tag_with_str.c2 == \"abca\" "
                     "and tag_with_str.c3 == \"bc\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {{6}};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
}

TEST_F(LookupTest, ConditionTest) {
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "create tag identity (BIRTHDAY int, NATION string, BIRTHPLACE_CITY string)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX idx_identity_cname_birth_gender_nation_city "
                    "ON identity(BIRTHDAY, NATION, BIRTHPLACE_CITY)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "INSERT VERTEX identity "
                    "(BIRTHDAY, NATION, BIRTHPLACE_CITY)"
                    "VALUES 1 : (19860413, \"汉族\", \"aaa\")";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "lookup on identity where "
                     "identity.NATION == \"汉族\" && "
                     "identity.BIRTHDAY > 19620101 && "
                     "identity.BIRTHDAY < 20021231 && "
                     "identity.BIRTHPLACE_CITY == \"bbb\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        ASSERT_TRUE(!resp.__isset.rows || resp.get_rows()->size() == 0);
    }
}

TEST_F(LookupTest, EdgeYieldVertexId) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE lookup_edge_1(col1, col2, col3) VALUES "
                     "200 -> 201@0:(201, 201, 201), "
                     "200 -> 202@0:(202, 202, 202)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 201 "
                     "YIELD lookup_edge_1._src";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking, VertexID>> expected = {
            {200, 201, 0, 200}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"SrcVID", "DstVID", "Ranking", "lookup_edge_1._src"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 201 "
                     "YIELD lookup_edge_1._dst";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking, VertexID>> expected = {
            {200, 201, 0, 201}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"SrcVID", "DstVID", "Ranking", "lookup_edge_1._dst"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 >= 201 "
                     "YIELD lookup_edge_1._src, lookup_edge_1._dst";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking, VertexID, VertexID>> expected = {
            {200, 201, 0, 200, 201},
            {200, 202, 0, 200, 202}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"SrcVID", "DstVID", "Ranking", "lookup_edge_1._src", "lookup_edge_1._dst"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
}

TEST_F(LookupTest, EdgeDistinctTest) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE lookup_edge_1(col1, col2, col3) VALUES "
                     "200 -> 201@0:(111, 201, 201), "
                     "200 -> 202@0:(111, 202, 202)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 111 "
                     "YIELD lookup_edge_1._src";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking, VertexID>> expected = {
            {200, 201, 0, 200},
            {200, 202, 0, 200}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"SrcVID", "DstVID", "Ranking", "lookup_edge_1._src"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 111 "
                     "YIELD DISTINCT lookup_edge_1._src";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking, VertexID>> expected = {
            {200, 201, 0, 200}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"SrcVID", "DstVID", "Ranking", "lookup_edge_1._src"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 111 "
                     "YIELD lookup_edge_1._src, lookup_edge_1.col1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking, VertexID, int64_t>> expected = {
            {200, 201, 0, 200, 111},
            {200, 202, 0, 200, 111}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"SrcVID", "DstVID", "Ranking", "lookup_edge_1._src", "lookup_edge_1.col1"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == 111 "
                     "YIELD DISTINCT lookup_edge_1._src, lookup_edge_1.col1";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking, VertexID, int64_t>> expected = {
            {200, 201, 0, 200, 111}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"SrcVID", "DstVID", "Ranking", "lookup_edge_1._src", "lookup_edge_1.col1"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
}

TEST_F(LookupTest, VertexDistinctTest) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX lookup_tag_1(col1, col2, col3) VALUES "
                     "200:(200, 222, 333), "
                     "201:(201, 222, 333), "
                     "202:(202, 202, 202)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 >= 200 "
                     "YIELD lookup_tag_1.col2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, int64_t>> expected = {
            {200, 222},
            {201, 222},
            {202, 202}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"VertexID"},
            {"lookup_tag_1.col2"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 >= 200 "
                     "YIELD DISTINCT lookup_tag_1.col2";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, int64_t>> expected = {
            {201, 222},
            {202, 202}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"VertexID"},
            {"lookup_tag_1.col2"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 >= 200 "
                     "YIELD lookup_tag_1.col2, lookup_tag_1.col3";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, int64_t, int64_t>> expected = {
            {200, 222, 333},
            {201, 222, 333},
            {202, 202, 202}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"VertexID"},
            {"lookup_tag_1.col2"},
            {"lookup_tag_1.col3"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 >= 200 "
                     "YIELD DISTINCT lookup_tag_1.col2, lookup_tag_1.col3";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, int64_t, int64_t>> expected = {
            {201, 222, 333},
            {202, 202, 202}
        };
        ASSERT_TRUE(verifyResult(resp, expected));
        std::vector<std::string> cols = {
            {"VertexID"},
            {"lookup_tag_1.col2"},
            {"lookup_tag_1.col3"}
        };
        ASSERT_EQ(cols, *resp.get_column_names());
    }
}
}   // namespace graph
}   // namespace nebula
