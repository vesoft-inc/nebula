/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/base/Base.h"
#include "graph/validator/test/ValidatorTestBase.h"

DECLARE_uint32(max_allowed_statements);

namespace nebula {
namespace graph {

class GetSubgraphValidatorTest : public ValidatorTestBase {
 public:
};

using PK = nebula::graph::PlanNode::Kind;

TEST_F(GetSubgraphValidatorTest, Base) {
  {
    std::string query = "GET SUBGRAPH FROM \"1\" YIELD vertices as nodes";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kStart,
        PK::kSubgraph,
        PK::kGetNeighbors,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GET SUBGRAPH WITH PROP 3 STEPS FROM \"1\" YIELD edges as relationships";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kStart,
        PK::kSubgraph,
        PK::kGetNeighbors,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GET SUBGRAPH  WITH PROP FROM \"1\" BOTH like YIELD vertices AS a";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kStart,
        PK::kSubgraph,
        PK::kGetNeighbors,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GET SUBGRAPH WITH PROP FROM \"1\", \"2\" IN like YIELD vertices as a, edges as b";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kStart,
        PK::kSubgraph,
        PK::kGetNeighbors,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(GetSubgraphValidatorTest, Input) {
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like._src AS src | GET SUBGRAPH WITH "
        "PROP FROM $-.src YIELD vertices as a, edges as b";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kDedup,
        PK::kSubgraph,
        PK::kProject,
        PK::kGetNeighbors,
        PK::kProject,
        PK::kStart,
        PK::kGetNeighbors,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "$a = GO FROM \"1\" OVER like YIELD like._src AS src; GET SUBGRAPH "
        "FROM $a.src YIELD vertices as a, edges as b";
    std::vector<PlanNode::Kind> expected = {
        PK::kDataCollect,
        PK::kLoop,
        PK::kDedup,
        PK::kSubgraph,
        PK::kProject,
        PK::kGetNeighbors,
        PK::kProject,
        PK::kStart,
        PK::kGetNeighbors,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GET SUBGRAPH 0 STEPS FROM \"1\" YIELD vertices as nodes";
    std::vector<PlanNode::Kind> expected = {
        PK::kAggregate,
        PK::kGetVertices,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GET SUBGRAPH WITH PROP 0 STEPS FROM \"1\", \"2\", \"3\" YIELD vertices as nodes";
    std::vector<PlanNode::Kind> expected = {
        PK::kAggregate,
        PK::kGetVertices,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like._src AS src | GET SUBGRAPH WITH "
        "PROP 0 STEPS FROM $-.src YIELD vertices as nodes";
    std::vector<PlanNode::Kind> expected = {
        PK::kAggregate,
        PK::kGetVertices,
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
        "$a = GO FROM \"1\" OVER like YIELD like._src AS src; GET SUBGRAPH "
        "WITH PROP 0 STEPS FROM $a.src YIELD vertices as nodes";
    std::vector<PlanNode::Kind> expected = {
        PK::kAggregate,
        PK::kGetVertices,
        PK::kDedup,
        PK::kProject,
        PK::kProject,
        PK::kGetNeighbors,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(GetSubgraphValidatorTest, invalidYield) {
  {
    std::string query = "GET SUBGRAPH WITH PROP FROM \"Tim Duncan\"";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: Missing yield clause.");
  }
  {
    std::string query = "GET SUBGRAPH WITH PROP FROM \"Tim Duncan\" YIELD vertice";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SemanticError: Get Subgraph only support YIELD vertices OR edges");
  }
  {
    std::string query = "GET SUBGRAPH WITH PROP FROM \"Tim Duncan\" YIELD vertices";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: please add alias when using `vertices'. near `vertices'");
  }
  {
    std::string query = "GET SUBGRAPH WITH PROP FROM \"Tim Duncan\" YIELD vertices as a, edge";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: please add alias when using `edge'. near `edge'");
  }
  {
    std::string query = "GET SUBGRAPH WITH PROP FROM \"Tim Duncan\" YIELD vertices as a, edges";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: please add alias when using `edges'. near `edges'");
  }
  {
    std::string query = "GET SUBGRAPH WITH PROP FROM \"Tim Duncan\" YIELD path";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: please add alias when using `path'. near `path'");
  }
  {
    std::string query = "GET SUBGRAPH WITH PROP FROM \"Tim Duncan\" YIELD 123";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SemanticError: Get Subgraph only support YIELD vertices OR edges");
  }
}

TEST_F(GetSubgraphValidatorTest, RefNotExist) {
  {
    std::string query = "GET SUBGRAPH WITH PROP FROM $-.id YIELD edges as b";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: `$-.id', not exist prop `id'");
  }
  {
    std::string query = "GET SUBGRAPH WITH PROP FROM $a.id YIELD edges as b";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: `$a.id', not exist variable `a'");
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD $$.person.age AS id | GET SUBGRAPH WITH "
        "PROP FROM $-.id YIELD vertices as nodes";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SemanticError: `$-.id', the srcs should be type of "
              "FIXED_STRING, but was`INT'");
  }
  {
    std::string query =
        "$a = GO FROM \"1\" OVER like YIELD $$.person.age AS ID; GET SUBGRAPH "
        "FROM $a.ID YIELD edges as relationships";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SemanticError: `$a.ID', the srcs should be type of "
              "FIXED_STRING, but was`INT'");
  }
  {
    std::string query =
        "$a = GO FROM \"1\" OVER like YIELD like._src AS src; GET SUBGRAPH "
        "WITH PROP FROM $b.src YIELD vertices as nodes";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: `$b.src', not exist variable `b'");
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like._dst AS id, like._src AS id | GET "
        "SUBGRAPH FROM $-.id YIELD edges as relationships";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: Duplicate Column Name : `id'");
  }
  {
    std::string query =
        "$a = GO FROM \"1\" OVER like YIELD like._dst AS id, like._src AS id; "
        "GET SUBGRAPH WITH PROP FROM $a.id YIELD vertices as nodes";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: Duplicate Column Name : `id'");
  }
}

}  // namespace graph
}  // namespace nebula
