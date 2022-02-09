/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/base/ObjectPool.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/validator/LookupValidator.h"
#include "graph/validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class LookupValidatorTest : public ValidatorTestBase {};

TEST_F(LookupValidatorTest, InputOutput) {
  // pipe
  {
    const std::string query =
        "LOOKUP ON person where person.age == 35 YIELD id(vertex) as id | "
        "FETCH PROP ON person $-.id YIELD vertex as node";
    EXPECT_TRUE(checkResult(query,
                            {
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kGetVertices,
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kTagIndexFullScan,
                                PlanNode::Kind::kStart,
                            }));
  }
  // pipe with yield
  {
    const std::string query =
        "LOOKUP ON person where person.age == 35 YIELD person.name AS name | "
        "FETCH PROP ON person $-.name YIELD vertex as node";
    EXPECT_TRUE(checkResult(query,
                            {
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kGetVertices,
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kTagIndexFullScan,
                                PlanNode::Kind::kStart,
                            }));
  }
  // variable
  {
    const std::string query =
        "$a = LOOKUP ON person where person.age == 35 YIELD id(vertex) as id; "
        "FETCH PROP ON person $a.id YIELD vertex as node";
    EXPECT_TRUE(checkResult(query,
                            {
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kGetVertices,
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kTagIndexFullScan,
                                PlanNode::Kind::kStart,
                            }));
  }
  // var with yield
  {
    const std::string query =
        "$a = LOOKUP ON person where person.age == 35 YIELD person.name AS name;"
        "FETCH PROP ON person $a.name YIELD vertex as node";
    EXPECT_TRUE(checkResult(query,
                            {
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kGetVertices,
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kTagIndexFullScan,
                                PlanNode::Kind::kStart,
                            }));
  }
}

TEST_F(LookupValidatorTest, InvalidYieldExpression) {
  {
    const std::string query = "LOOKUP ON person where person.age > 20;";
    EXPECT_FALSE(checkResult(query, {}));
  }
  {
    const std::string query =
        "LOOKUP ON person where person.age == 35 YIELD person.age + 1 AS age;";
    EXPECT_TRUE(checkResult(query,
                            {
                                PlanNode::Kind::kProject,
                                PlanNode::Kind::kFilter,
                                PlanNode::Kind::kTagIndexFullScan,
                                PlanNode::Kind::kStart,
                            }));
  }
}

TEST_F(LookupValidatorTest, InvalidFilterExpression) {
  {
    const std::string query =
        "LOOKUP ON person where person.age == person.name YIELD vertex as node;";
    EXPECT_FALSE(checkResult(query, {}));
  }
  {
    const std::string query =
        "LOOKUP ON person where person.age > person.name YIELD vertex as node;";
    EXPECT_FALSE(checkResult(query, {}));
  }
  {
    const std::string query =
        "LOOKUP ON person where person.age != person.name YIELD vertex as node;";
    EXPECT_FALSE(checkResult(query, {}));
  }
  {
    const std::string query = "LOOKUP ON person where person.age + 1 > 5 YIELD person.age;";
    EXPECT_FALSE(checkResult(query, {}));
  }
  {
    const std::string query =
        "LOOKUP ON person where person.age > person.name + 5 YIELD id(vertex);";
    EXPECT_FALSE(checkResult(query, {}));
  }
  {
    const std::string query = "LOOKUP ON person where  1 + 5 < person.age YIELD vertex as node;";
    EXPECT_TRUE(checkResult(query, {}));
  }
  {
    const std::string query = "LOOKUP ON person where person.age > 1 + 5 YIELD vertex as node;";
    EXPECT_TRUE(checkResult(query, {}));
  }
  {
    const std::string query = "LOOKUP ON person where person.age > abs(-5) YIELD id(vertex);";
    EXPECT_TRUE(checkResult(query, {}));
  }
}

TEST_F(LookupValidatorTest, wrongYield) {
  {
    std::string query = "LOOKUP ON person";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: Missing yield clause.");
  }
  {
    std::string query = "LOOKUP ON person YIELD vertex";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: please add alias when using `vertex'. near `vertex'");
  }
  {
    std::string query = "LOOKUP ON person YIELD count(*)";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: Invalid use of aggregating function in yield clause. near `count(*)'");
  }
  {
    std::string query = "LOOKUP ON person YIELD vertex as node, edge";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: please add alias when using `edge'. near `edge'");
  }
  {
    std::string query = "LOOKUP ON person YIELD edge as e";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: illegal yield clauses `EDGE AS e'");
  }
  {
    std::string query = "LOOKUP ON person YIELD vertex as node, player.age";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: Schema name error: player");
  }
}

}  // namespace graph
}  // namespace nebula
