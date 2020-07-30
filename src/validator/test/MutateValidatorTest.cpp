/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

using PK = nebula::graph::PlanNode::Kind;
class MutateValidatorTest : public ValidatorTestBase {
};

TEST_F(MutateValidatorTest, InsertVertexTest) {
    // wrong schema
    {
        auto cmd = "INSERT VERTEX person(name, age2) VALUES \"A\":(\"a\", 19);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
}

TEST_F(MutateValidatorTest, InsertEdgeTest) {
    // wrong schema
    {
        auto cmd = "INSERT EDGE like(start, end2) VALUES \"A\"->\"B\":(11, 11);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
}

TEST_F(MutateValidatorTest, DeleteVertexTest) {
    // succeed
    {
        auto cmd = "DELETE VERTEX \"A\"";
        std::vector<PlanNode::Kind> expected = {
            PK::kDeleteVertices,
            PK::kDeleteEdges,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // pipe
    {
        auto cmd = "GO FROM \"C\" OVER like YIELD like._dst as dst | DELETE VERTEX $-.dst";
        std::vector<PlanNode::Kind> expected = {
            PK::kDeleteVertices,
            PK::kDeleteEdges,
            PK::kGetNeighbors,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // pipe wrong input
    {
        auto cmd = "GO FROM \"C\" OVER E YIELD E._dst as dst | DELETE VERTEX $-.a";
        ASSERT_FALSE(checkResult(cmd));
    }
}

TEST_F(MutateValidatorTest, DeleteEdgeTest) {
    // succeed
    {
        auto cmd = "DELETE EDGE like \"A\"->\"B\"";
        std::vector<PlanNode::Kind> expected = {
            PK::kDeleteEdges,
            PK::kStart,
        };
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // not existed edge name
    {
        auto cmd = "DELETE EDGE study \"A\"->\"B\"";
        ASSERT_FALSE(checkResult(cmd));
    }
    // pipe
    {
        auto cmd = "GO FROM \"C\" OVER like "
                   "YIELD like._src as src, like._dst as dst, like._rank as rank "
                   "| DELETE EDGE like $-.src -> $-.dst @ $-.rank";
        std::vector<PlanNode::Kind> expected = {
            PK::kDeleteEdges,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // var
    {
        auto cmd = "$var = GO FROM \"C\" OVER like "
                   "YIELD like._src as src, like._dst as dst, like._rank as rank "
                   "; DELETE EDGE like $var.src -> $var.dst @ $var.rank";
        std::vector<PlanNode::Kind> expected = {
            PK::kDeleteEdges,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(checkResult(cmd, expected));
    }
    // pipe wrong input
    {
        auto cmd = "GO FROM \"C\" OVER like "
                   "YIELD like._src as src, like._dst as dst, like._rank as rank "
                   "| DELETE EDGE like $-.dd -> $-.dst @ $-.rank";
        ASSERT_FALSE(checkResult(cmd));
    }
}
}  // namespace graph
}  // namespace nebula
