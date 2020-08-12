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
                     "200:(\"col1_200\", \"col2_200\", \"col3_200\"), "
                     "201:(\"col1_201\", \"col2_201\", \"col3_201\"), "
                     "202:(\"col1_202\", \"col2_202\", \"col3_202\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        /**
         * kPrimary == kPrimary
         */
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE col1 == \"col1_200\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == \"col1\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == \"col1_200\"";
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
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col1 == \"col1_200\" "
                     "YIELD lookup_tag_1.col1, lookup_tag_1.col2, lookup_tag_1.col3";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, std::string, std::string, std::string>> expected = {
            {200, "col1_200", "col2_200", "col3_200"},
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
                     "200 -> 201@0:(\"col1_200_1\", \"col2_200_1\", \"col3_200_1\"), "
                     "200 -> 202@0:(\"col1_200_2\", \"col2_200_2\", \"col3_200_2\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        /**
         * kPrimary == kPrimary
         */
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE col1 == \"col1_200_1\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == \"col1\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking>> expected = {};
        ASSERT_TRUE(verifyResult(resp, expected));
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == \"col1_200_1\"";
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
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col1 == \"col1_200_1\" "
                     "YIELD lookup_edge_1.col1, lookup_edge_1.col2, lookup_edge_1.col3";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
        std::vector<std::tuple<VertexID, VertexID, EdgeRanking,
                               std::string, std::string, std::string>> expected = {
            {200, 201, 0, "col1_200_1", "col2_200_1", "col3_200_1"},
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
                     "200:(\"col1_200\", \"col2_200\", \"col3_200\"), "
                     "201:(\"col1_201\", \"col2_201\", \"col3_201\"), "
                     "202:(\"col1_202\", \"col2_202\", \"col3_202\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_tag_1 WHERE lookup_tag_1.col2 == \"col2_200\"";
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
        auto query = "LOOKUP ON lookup_tag_2 WHERE lookup_tag_2.col1 == \"col2_200\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(LookupTest, EdgeIndexHint) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE lookup_edge_1(col1, col2, col3) VALUES "
                     "200 -> 201@0:(\"col1_200_1\", \"col2_200_1\", \"col3_200_1\"), "
                     "200 -> 202@0:(\"col1_200_2\", \"col2_200_2\", \"col3_200_2\")";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON lookup_edge_1 WHERE lookup_edge_1.col2 == \"col2_200_1\"";
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
        auto query = "LOOKUP ON lookup_edge_2 WHERE lookup_edge_2.col1 == \"col2_200\"";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
    }
}

TEST_F(LookupTest, VertexConditionScan) {
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX lookup_tag_2(col1, col2, col3, col4) VALUES "
                     "220:(\"col1_220\", 100, 100.5, true), "
                     "221:(\"col1_221\", 200, 200.5, true), "
                     "222:(\"col1_222\", 300, 300.5, true), "
                     "223:(\"col1_223\", 400, 400.5, true), "
                     "224:(\"col1_224\", 500, 500.5, true), "
                     "225:(\"col1_225\", 600, 600.5, true)";
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
                     "220:(\"col1_220\", 100, 100.5, true), "
                     "221:(\"col1_221\", 200, 200.5, true), "
                     "222:(\"col1_222\", 300, 300.5, true), "
                     "223:(\"col1_223\", 400, 400.5, true), "
                     "224:(\"col1_224\", 500, 500.5, true), "
                     "225:(\"col1_225\", 600, 600.5, true)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT EDGE lookup_edge_2(col1, col2, col3, col4) VALUES "
                     "220 -> 221@0:(\"col1_220\", 100, 100.5, true), "
                     "220 -> 222@0:(\"col1_221\", 200, 200.5, true), "
                     "220 -> 223@0:(\"col1_222\", 300, 300.5, true), "
                     "220 -> 224@0:(\"col1_223\", 400, 400.5, true), "
                     "220 -> 225@0:(\"col1_224\", 500, 500.5, true)";
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
                     "220:(\"col1_220\", 100, 100.5, true), "
                     "221:(\"col1_221\", 200, 200.5, true), "
                     "222:(\"col1_222\", 300, 300.5, true), "
                     "223:(\"col1_223\", 400, 400.5, true), "
                     "224:(\"col1_224\", 500, 500.5, true), "
                     "225:(\"col1_225\", 600, 600.5, true)";
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
        ASSERT_EQ(cpp2::ErrorCode::E_EXECUTION_ERROR, code);
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
        auto stmt = "CREATE TAG student(name string, age int)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG INDEX student_index ON student(name, age)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto stmt = "CREATE TAG teacher(name string, age int)";
        auto code = client_->execute(stmt, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    sleep(FLAGS_heartbeat_interval_secs + 1);
    {
        cpp2::ExecutionResponse resp;
        auto query = "INSERT VERTEX student(name, age), teacher(name, age)  VALUES "
                     "220:(\"student_1\", 20, \"teacher_1\", 30), "
                     "221:(\"student_2\", 22, \"teacher_1\", 32)";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
    }
    // Invalid tag name in yield clause.
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON student WHERE student.name == \"student_1\" YIELD teacher.age";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Invalid tag name in yield clause. and Alias is same with tag name.
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON student WHERE student.name == \"student_1\" YIELD teacher.age"
                     " as student_name";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    // Invalid tag name in where clause.
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON student WHERE teacher.name == \"student_1\" YIELD student.age";
        auto code = client_->execute(query, resp);
        ASSERT_EQ(cpp2::ErrorCode::E_SYNTAX_ERROR, code);
    }
    {
        cpp2::ExecutionResponse resp;
        auto query = "LOOKUP ON student WHERE student.name == \"student_1\" YIELD student.age";
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
    auto executor = std::make_unique<LookupExecutor>(nullptr, nullptr);
    std::map<std::string, IndexID> expected;
    {
        executor->indexes_ = indexes.value();
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
        ASSERT_TRUE(executor->findValidIndex().ok());
        ASSERT_TRUE(expected["i1"] == executor->index_ ||
                    expected["i4"] == executor->index_ ||
                    expected["i5"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c1 == 1 and c2 > 1"; expected i1 or i4 i4 or i5
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::GT);
        ASSERT_TRUE(executor->findValidIndex().ok());
        ASSERT_TRUE(expected["i1"] == executor->index_ ||
                    expected["i4"] == executor->index_ ||
                    expected["i5"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c1 > 1 and c2 == 1"; expected i2
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::GT);
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::EQ);
        ASSERT_TRUE(executor->findValidIndex().ok());
        ASSERT_TRUE(expected["i2"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c1 > 1 and c2 == 1 and c3 == 1"; expected i4 or i5
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::GT);
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::EQ);
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::EQ);
        ASSERT_TRUE(executor->findValidIndex().ok());
        ASSERT_TRUE(expected["i4"] == executor->index_ || expected["i5"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c3 > 1"; expected i3
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::GT);
        ASSERT_TRUE(executor->findValidIndex().ok());
        ASSERT_TRUE(expected["i3"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c3 > 1 and c1 > 1"; expected i4 or i5.
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::GT);
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::GT);
        ASSERT_TRUE(executor->findValidIndex().ok());
        ASSERT_TRUE(expected["i4"] == executor->index_ || expected["i5"] == executor->index_);
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c4 > 1"; No invalid index found.
        executor->filters_.emplace_back("c4", RelationalExpression::Operator::GT);
        ASSERT_FALSE(executor->findValidIndex().ok());
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c2 > 1 and c3 > 1"; No invalid index found.
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::GT);
        executor->filters_.emplace_back("c3", RelationalExpression::Operator::GT);
        ASSERT_FALSE(executor->findValidIndex().ok());
        executor->filters_.clear();
    }
    {
        // "LOOKUP on t1 WHERE t1.c2 > 1 and c1 != 1"; expected i2.
        executor->filters_.emplace_back("c2", RelationalExpression::Operator::GT);
        executor->filters_.emplace_back("c1", RelationalExpression::Operator::NE);
        ASSERT_TRUE(executor->findValidIndex().ok());
        ASSERT_TRUE(expected["i2"] == executor->index_);
        executor->filters_.clear();
    }
}

}   // namespace graph
}   // namespace nebula
