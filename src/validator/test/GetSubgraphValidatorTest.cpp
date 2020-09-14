/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include "validator/test/ValidatorTestBase.h"

DECLARE_uint32(max_allowed_statements);

namespace nebula {
namespace graph {

class GetSubgraphValidatorTest : public ValidatorTestBase {
public:
};

using PK = nebula::graph::PlanNode::Kind;

TEST_F(GetSubgraphValidatorTest, Base) {
    {
        std::string query = "GET SUBGRAPH FROM \"1\"";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kFilter,
            PK::kGetNeighbors,
            PK::kLoop,
            PK::kStart,
            PK::kAggregate,
            PK::kDedup,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GET SUBGRAPH 3 STEPS FROM \"1\"";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kFilter,
            PK::kGetNeighbors,
            PK::kLoop,
            PK::kStart,
            PK::kAggregate,
            PK::kDedup,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GET SUBGRAPH FROM \"1\" BOTH like";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kFilter,
            PK::kGetNeighbors,
            PK::kLoop,
            PK::kStart,
            PK::kAggregate,
            PK::kDedup,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GET SUBGRAPH FROM \"1\", \"2\" IN like";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kFilter,
            PK::kGetNeighbors,
            PK::kLoop,
            PK::kStart,
            PK::kAggregate,
            PK::kDedup,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
}

TEST_F(GetSubgraphValidatorTest, Input) {
    {
        std::string query =
            "GO FROM \"1\" OVER like YIELD like._src AS src | GET SUBGRAPH FROM $-.src";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kFilter,
            PK::kGetNeighbors,
            PK::kLoop,
            PK::kAggregate,
            PK::kAggregate,
            PK::kDedup,
            PK::kDedup,
            PK::kProject,
            PK::kProject,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kGetNeighbors,
            PK::kStart,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query =
            "$a = GO FROM \"1\" OVER like YIELD like._src AS src; GET SUBGRAPH FROM $a.src";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kFilter,
            PK::kGetNeighbors,
            PK::kLoop,
            PK::kAggregate,
            PK::kAggregate,
            PK::kDedup,
            PK::kDedup,
            PK::kProject,
            PK::kProject,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kGetNeighbors,
            PK::kStart,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GET SUBGRAPH 0 STEPS FROM \"1\"";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kGetVertices,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "GET SUBGRAPH 0 STEPS FROM \"1\", \"2\", \"3\"";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kGetVertices,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query =
            "GO FROM \"1\" OVER like YIELD like._src AS src | GET SUBGRAPH 0 STEPS FROM $-.src";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kGetVertices,
            PK::kAggregate,
            PK::kDedup,
            PK::kProject,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query =
            "$a = GO FROM \"1\" OVER like YIELD like._src AS src; GET SUBGRAPH 0 STEPS FROM $a.src";
        std::vector<PlanNode::Kind> expected = {
            PK::kAggregate,
            PK::kGetVertices,
            PK::kAggregate,
            PK::kDedup,
            PK::kProject,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
}

TEST_F(GetSubgraphValidatorTest, RefNotExist) {
    {
        std::string query = "GET SUBGRAPH FROM $-.id";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SemanticError: `$-.id', not exist prop `id'");
    }
    {
        std::string query = "GET SUBGRAPH FROM $a.id";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SemanticError: `$a.id', not exist variable `a'");
    }
    {
        std::string query =
            "GO FROM \"1\" OVER like YIELD $$.person.age AS id | GET SUBGRAPH FROM $-.id";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `$-.id', the srcs should be type of string, but was`INT'");
    }
    {
        std::string query =
            "$a = GO FROM \"1\" OVER like YIELD $$.person.age AS ID; GET SUBGRAPH FROM $a.ID";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `$a.ID', the srcs should be type of string, but was`INT'");
    }
    {
        std::string query =
            "$a = GO FROM \"1\" OVER like YIELD like._src AS src; GET SUBGRAPH FROM $b.src";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SemanticError: `$b.src', not exist variable `b'");
    }
    // {
    //     std::string query =
    //         "GO FROM \"1\" OVER like YIELD like._dst AS id, like._src AS id | GET SUBGRAPH FROM
    //         $-.id";
    //     auto result = checkResult(query);
    //     EXPECT_EQ(std::string(result.message()), "SyntaxError:");
    // }
    // {
    //     std::string query = "$a = GO FROM \"1\" OVER like YIELD like._dst AS id, like._src AS id;
    //     GET "
    //                         "SUBGRAPH FROM $a.id";
    //     auto result = checkResult(query);
    //     EXPECT_EQ(std::string(result.message()), "SemanticError:");
    // }
}

}  // namespace graph
}  // namespace nebula
