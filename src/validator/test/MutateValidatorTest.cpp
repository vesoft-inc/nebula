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
    // wrong vid expression
    {
        auto cmd = "INSERT VERTEX person(name, age2) VALUES hash($-,name):(\"a\", 19);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // vid use function call
    {
        auto cmd = "INSERT VERTEX person(name, age) VALUES lower(\"TOM\"):(\"a\", 19);";
        ASSERT_TRUE(checkResult(cmd, { PK::kInsertVertices, PK::kStart }));
    }
}

TEST_F(MutateValidatorTest, InsertEdgeTest) {
    // wrong schema
    {
        auto cmd = "INSERT EDGE like(start, end2) VALUES \"A\"->\"B\":(11, 11);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // wrong vid expression
    {
        auto cmd = "INSERT EDGE like(start, end) VALUES hash($-,name)->\"Tom\":(2010, 2020);";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // vid use function call
    {
        auto cmd = "INSERT EDGE like(start, end) VALUES lower(\"Lily\")->\"Tom\":(2010, 2020);";
        ASSERT_TRUE(checkResult(cmd, { PK::kInsertEdges, PK::kStart }));
    }
}

TEST_F(MutateValidatorTest, DeleteVertexTest) {
    // succeed
    {
        auto cmd = "DELETE VERTEX \"A\"";
        std::vector<PlanNode::Kind> expected = {
            PK::kDeleteVertices,
            PK::kDeleteEdges,
            PK::kDedup,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kDedup,
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
            PK::kDedup,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kDedup,
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
            PK::kDedup,
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
            PK::kDedup,
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
            PK::kDedup,
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

TEST_F(MutateValidatorTest, UpdateVertexTest) {
    // not exist tag
    {
        auto cmd = "UPDATE VERTEX ON student \"Tom\" SET count = 1";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // multi tags
    {
        auto cmd = "UPDATE VERTEX \"Tom\" SET person.count = 1, student.age = $^.student.age + 1";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // wrong expr
    {
        auto cmd = "UPDATE VERTEX \"Tom\" SET person.age = person.age + 1";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // with function
    {
        auto cmd = "UPDATE VERTEX ON person \"Tom\" SET age = abs(age + 1)";
        ASSERT_TRUE(checkResult(cmd, {}));
    }
    // 1.0 syntax succeed
    {
        auto cmd = "UPDATE VERTEX \"Tom\""
                   "SET person.age = $^.person.age + 1 "
                   "WHEN $^.person.age == 18 "
                   "YIELD $^.person.name AS name, $^.person.age AS age";
        ASSERT_TRUE(checkResult(cmd, {PK::kUpdateVertex, PK::kStart}));
    }
    // 2.0 syntax succeed
    {
        auto cmd = "UPDATE VERTEX ON person \"Tom\""
                   "SET age = age + 1 "
                   "WHEN age == 18 "
                   "YIELD name AS name, age AS age";
        ASSERT_TRUE(checkResult(cmd, {PK::kUpdateVertex, PK::kStart}));
    }
}

TEST_F(MutateValidatorTest, UpdateEdgeTest) {
    // not exist edge
    {
        auto cmd = "UPDATE EDGE ON study \"Tom\"->\"Lily\" SET count = 1";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // Wrong expr "$^.peson.age"
    {
        auto cmd = "UPDATE EDGE \"Tom\"->\"Lily\" OF like "
                   "SET end = like.end + 1 "
                   "WHEN $^.peson.age >= 18 "
                   "YIELD $^.peson.age AS age, like.end AS end";
        ASSERT_FALSE(checkResult(cmd, {}));
    }
    // 1.0 syntax succeed
    {
        auto cmd = "UPDATE EDGE \"Tom\"->\"Lily\" OF like "
                   "SET end = like.end + 1 "
                   "WHEN like.start >= 2010 "
                   "YIELD like.start AS start, like.end AS end";
        ASSERT_TRUE(checkResult(cmd, {PK::kUpdateEdge, PK::kUpdateEdge, PK::kStart}));
    }
    // 2.0 syntax succeed
    {
        auto cmd = "UPDATE EDGE ON like \"Tom\"->\"Lily\""
                   "SET end = end + 1 "
                   "WHEN start >= 2010 "
                   "YIELD start AS start, end AS end";
        ASSERT_TRUE(checkResult(cmd, {PK::kUpdateEdge, PK::kUpdateEdge, PK::kStart}));
    }
}
}  // namespace graph
}  // namespace nebula
