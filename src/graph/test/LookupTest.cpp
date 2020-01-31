/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "graph/test/LookupTestBase.h"

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
}   // namespace graph
}   // namespace nebula
