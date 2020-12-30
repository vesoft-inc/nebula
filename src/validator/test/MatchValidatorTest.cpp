/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/MatchValidator.h"

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class MatchValidatorTest : public ValidatorTestBase {};

TEST_F(MatchValidatorTest, SeekByTagIndex) {
    // empty properties index
    {
        std::string query = "MATCH (v:person) RETURN id(v) AS id;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kProject,
                                                // TODO this tag filter could remove in this case
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    // non empty properties index
    {
        std::string query = "MATCH (v:book) RETURN id(v) AS id;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kProject,
                                                // TODO this tag filter could remove in this case
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    // non empty properties index with extend
    {
        std::string query = "MATCH (p:person)-[:like]->(b:book) RETURN b.name AS book;";
        std::vector<PlanNode::Kind> expected = {PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kDataJoin,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetVertices,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kPassThrough,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kFilter,
                                                PlanNode::Kind::kGetNeighbors,
                                                PlanNode::Kind::kDedup,
                                                PlanNode::Kind::kProject,
                                                PlanNode::Kind::kIndexScan,
                                                PlanNode::Kind::kStart};
        EXPECT_TRUE(checkResult(query, expected));
    }
    // non index
    {
        std::string query = "MATCH (v:room) RETURN id(v) AS id;";
        EXPECT_FALSE(validate(query));
    }
}

}   // namespace graph
}   // namespace nebula
