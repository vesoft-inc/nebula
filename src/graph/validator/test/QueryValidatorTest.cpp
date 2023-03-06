/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/base/Base.h"
#include "graph/validator/test/ValidatorTestBase.h"

DECLARE_uint32(max_allowed_statements);
DECLARE_uint32(max_allowed_query_size);

namespace nebula {
namespace graph {

class QueryValidatorTest : public ValidatorTestBase {
 public:
};

using PK = nebula::graph::PlanNode::Kind;

TEST_F(QueryValidatorTest, TestFirstSentence) {
  auto testFirstSentence = [](const std::string &msg) -> bool {
    return msg.find_first_of("SyntaxError: Could not start with the statement") == 0;
  };

  {
    auto result = checkResult("LIMIT 2, 10");
    EXPECT_FALSE(result);
    EXPECT_TRUE(testFirstSentence(result.message()));
  }
  {
    auto result = checkResult("LIMIT 2, 10 | YIELD 2");
    EXPECT_TRUE(testFirstSentence(result.message()));
  }
  {
    auto result = checkResult("LIMIT 2, 10 | YIELD 2 | YIELD 3");
    EXPECT_TRUE(testFirstSentence(result.message()));
  }
  {
    auto result = checkResult("ORDER BY 1");
    EXPECT_TRUE(testFirstSentence(result.message()));
  }
  {
    auto result = checkResult("GROUP BY 1");
    EXPECT_TRUE(testFirstSentence(result.message()));
  }
}

TEST_F(QueryValidatorTest, GoZeroStep) {
  {
    std::string query = "GO 0 STEPS FROM \"1\" OVER serve YIELD edge as e";
    std::vector<PlanNode::Kind> expected = {PK::kPassThrough, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 0 STEPS FROM \"1\" OVER like YIELD like._dst as id"
        "| GO FROM $-.id OVER serve YIELD edge as e";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kPassThrough,
                                            PK::kExpandAll,
                                            PK::kStart,
                                            PK::kExpand,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO 0 TO 0 STEPS FROM \"1\" OVER serve YIELD $$ as dst";
    std::vector<PlanNode::Kind> expected = {PK::kPassThrough, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, GoNSteps) {
  {
    std::string query = "GO 2 STEPS FROM \"1\" OVER like YIELD $^ as src";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 3 STEPS FROM \"1\",\"2\",\"3\" OVER like WHERE like.likeness > 90 YIELD $^ as src";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject,
        PK::kFilter,
        PK::kExpandAll,
        PK::kExpand,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 3 steps FROM \"1\",\"2\",\"3\" OVER like WHERE $^.person.age > 20"
        "YIELD distinct $^.person.name";
    std::vector<PlanNode::Kind> expected = {
        PK::kDedup,
        PK::kProject,
        PK::kFilter,
        PK::kExpandAll,
        PK::kExpand,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 2 STEPS FROM \"1\",\"2\",\"3\" OVER like WHERE $^.person.age > 20"
        "YIELD distinct $^.person.name ";
    std::vector<PlanNode::Kind> expected = {
        PK::kDedup, PK::kProject, PK::kFilter, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, GoWithPipe) {
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id | GO 2 STEPS FROM $-.id OVER like YIELD edge as e";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kArgument,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 2 STEPS FROM \"1\" OVER like YIELD like._dst AS id"
        "| GO 1 STEPS FROM $-.id OVER like YIELD src(edge) as src";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kArgument,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "YIELD \"1\" AS id | GO FROM $-.id OVER like YIELD id($^) as id";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kStart,
                                            PK::kExpand,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "YIELD \"1\" AS id | GO FROM $-.id OVER like YIELD id($$) as id";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kStart,
                                            PK::kExpand,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO FROM 'Tim' OVER * YIELD id($$) as id";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id | GO 1 STEPS FROM $-.id OVER like YIELD $-.id, like._dst";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kArgument,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id | GO 1 STEPS FROM $-.id OVER like "
        "WHERE $-.id == \"2\" YIELD $-.id, like._dst";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kFilter,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kArgument,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id | GO 1 STEPS FROM $-.id OVER like "
        "WHERE $-.id == \"2\" YIELD DISTINCT $-.id, like._dst";
    std::vector<PlanNode::Kind> expected = {PK::kDedup,
                                            PK::kProject,
                                            PK::kFilter,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kArgument,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id | GO 2 STEPS FROM $-.id OVER like YIELD $-.id, like._dst";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kArgument,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id | GO 2 STEPS FROM $-.id OVER like "
        "WHERE $-.id == \"2\" YIELD $-.id, like._dst";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kFilter,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kArgument,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id | GO 2 STEPS FROM $-.id OVER like "
        "WHERE $-.id == \"2\" YIELD DISTINCT $-.id, like._dst";
    std::vector<PlanNode::Kind> expected = {PK::kDedup,
                                            PK::kProject,
                                            PK::kFilter,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kArgument,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id, $$.person.name as name | GO 1 STEPS FROM $-.id OVER like "
        "YIELD $-.name, like.likeness + 1, $-.id, like._dst, "
        "$$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kArgument,
                                            PK::kArgument,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id, $$.person.name as name | GO 1 STEPS FROM $-.id OVER like "
        "YIELD DISTINCT $-.name, like.likeness + 1, $-.id, like._dst, "
        "$$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kDedup,
                                            PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kArgument,
                                            PK::kArgument,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like "
        "YIELD $^.person.name AS name, like._dst AS id "
        "| GO FROM $-.id OVER like "
        "YIELD $-.name, $^.person.name, $$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id, $$.person.name as name | GO 2 STEPS FROM $-.id OVER like "
        "YIELD $-.name, like.likeness + 1, $-.id, like._dst, "
        "$$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kArgument,
                                            PK::kArgument,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id, $$.person.name as name | GO 2 STEPS FROM $-.id OVER like "
        "YIELD DISTINCT $-.name, like.likeness + 1, $-.id, like._dst, "
        "$$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kDedup,
                                            PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kArgument,
                                            PK::kArgument,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 STEPS FROM \"1\" OVER like YIELD like._dst AS "
        "id | GO 1 STEPS FROM \"1\" OVER like YIELD like._dst";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, GoWithVariable) {
  {
    std::string query =
        "$var = GO FROM \"1\" OVER like "
        "YIELD $^.person.name AS name, like._dst AS id;"
        "GO FROM $var.id OVER like "
        "YIELD $var.name, $^.person.name, $$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, GoReversely) {
  {
    std::string query = "GO FROM \"1\" OVER like REVERSELY YIELD $$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO 2 STEPS FROM \"1\" OVER like REVERSELY YIELD $$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, GoBidirectly) {
  {
    std::string query = "GO FROM \"1\" OVER like BIDIRECT YIELD edge as e";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like BIDIRECT "
        "YIELD $$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, GoOneStep) {
  {
    std::string query = "GO FROM \"1\" OVER like YIELD src(edge) as src";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO FROM \"1\" OVER like REVERSELY YIELD dst(edge) as dst";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO FROM \"1\" OVER like BIDIRECT YIELD dst(edge) as dst";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO FROM \"1\" OVER like YIELD like.start";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like "
        "YIELD $^.person.name,$^.person.age";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like "
        "YIELD $$.person.name,$$.person.age";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like "
        "YIELD $^.person.name, like._dst, "
        "$$.person.name, $$.person.age + 1";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like "
        "WHERE like._dst == \"2\""
        "YIELD $^.person.name, like._dst, "
        "$$.person.name, $$.person.age + 1";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kFilter,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like "
        "WHERE like._dst == \"2\""
        "YIELD DISTINCT $^.person.name, like._dst, "
        "$$.person.name, $$.person.age + 1";
    std::vector<PlanNode::Kind> expected = {PK::kDedup,
                                            PK::kProject,
                                            PK::kFilter,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO FROM \"1\",\"2\",\"3\" OVER like YIELD edge as e";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\",\"2\",\"3\" OVER like WHERE like.likeness > 90 YIELD edge as e";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject, PK::kFilter, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\",\"2\",\"3\" OVER like WHERE $^.person.age > 20"
        "YIELD distinct $^.person.name ";
    std::vector<PlanNode::Kind> expected = {
        PK::kDedup, PK::kProject, PK::kFilter, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\",\"2\",\"3\" OVER like WHERE $^.person.name == \"me\" YIELD edge as e";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject, PK::kFilter, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like._dst AS id"
        "| GO FROM $-.id OVER like YIELD dst(edge) as dst";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kArgument,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, GoOverAll) {
  {
    std::string query = "GO FROM \"1\" OVER * REVERSELY YIELD serve._src, like._src";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO FROM \"1\" OVER * REVERSELY YIELD edge as e";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO FROM \"1\" OVER * YIELD src(edge) as src";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO FROM \"1\" OVER * YIELD $$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, OutputToAPipe) {
  {
    std::string query =
        "GO FROM '1' OVER like YIELD like._dst as id "
        "| ( GO FROM $-.id OVER like YIELD like._dst as id | GO FROM $-.id "
        "OVER serve YIELD dst(edge) as dst )";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kHashInnerJoin,
                                            PK::kExpand,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kArgument,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kArgument,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, GoMToN) {
  {
    std::string query = "GO 1 TO 2 STEPS FROM '1' OVER like YIELD DISTINCT like._dst";
    std::vector<PlanNode::Kind> expected = {
        PK::kDedup, PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO 0 TO 2 STEPS FROM '1' OVER like YIELD DISTINCT like._dst";
    std::vector<PlanNode::Kind> expected = {
        PK::kDedup, PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 TO 2 STEPS FROM '1' OVER like "
        "YIELD DISTINCT like._dst, like.likeness, $$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kDedup,
                                            PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO 1 TO 2 STEPS FROM '1' OVER like REVERSELY YIELD DISTINCT like._dst";
    std::vector<PlanNode::Kind> expected = {
        PK::kDedup, PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO 1 TO 2 STEPS FROM '1' OVER like BIDIRECT YIELD DISTINCT like._dst";
    std::vector<PlanNode::Kind> expected = {
        PK::kDedup, PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "GO 1 TO 2 STEPS FROM '1' OVER * YIELD serve._dst, like._dst";
    std::vector<PlanNode::Kind> expected = {PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO 1 TO 2 STEPS FROM '1' OVER * "
        "YIELD serve._dst, like._dst, serve.start, like.likeness, "
        "$$.person.name";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashLeftJoin,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kGetVertices,
                                            PK::kStart,
                                            PK::kArgument};
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "GO FROM 'Tim Duncan' OVER like YIELD like._src as src, like._dst as "
        "dst "
        "| GO 1 TO 2 STEPS FROM $-.src OVER like YIELD $-.src as src, "
        "like._dst as dst";
    std::vector<PlanNode::Kind> expected = {PK::kProject,
                                            PK::kHashInnerJoin,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kArgument,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, GoInvalid) {
  {
    // friend not exist.
    std::string query = "GO FROM \"1\" OVER friend YIELD $$ as dst";
    EXPECT_FALSE(checkResult(query));
  }
  {
    // manager not exist
    std::string query = "GO FROM \"1\" OVER like YIELD $^.manager.name,$^.person.age";
    EXPECT_FALSE(checkResult(query));
  }
  {
    // manager not exist
    std::string query = "GO FROM \"1\" OVER like YIELD $$.manager.name,$$.person.age";
    EXPECT_FALSE(checkResult(query));
  }
  {
    // column not exist
    std::string query =
        "GO FROM \"1\" OVER like YIELD like._dst AS id | GO FROM $-.col OVER like YIELD edge as e";
    EXPECT_FALSE(checkResult(query));
  }
  {
    // invalid id type
    std::string query =
        "GO FROM \"1\" OVER like YIELD like.likeness AS id"
        "| GO FROM $-.id OVER like YIELD edge as e";
    EXPECT_FALSE(checkResult(query));
  }
  {
    // multi inputs
    std::string query =
        "$var = GO FROM \"2\" OVER like;"
        "GO FROM \"1\" OVER like YIELD like._dst AS id"
        "| GO FROM $-.id OVER like WHERE $var.id == \"\" YIELD edge as e";
    EXPECT_FALSE(checkResult(query));
  }
  {
    // yield agg without groupBy is not supported
    std::string query = "GO FROM \"2\" OVER like YIELD COUNT(123);";
    auto result = checkResult(query);
    EXPECT_EQ(
        std::string(result.message()),
        "SyntaxError: Invalid use of aggregating function in yield clause. near `COUNT(123)'");
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like._dst AS id, like._src AS id | GO "
        "FROM $-.id OVER like YIELD like._dst as dst";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: Duplicate Column Name : `id'");
  }
  {
    std::string query =
        "$a = GO FROM \"1\" OVER like YIELD like._dst AS id, like._src AS id; "
        "GO FROM $a.id OVER like YIELD dst(edge) as dst";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: Duplicate Column Name : `id'");
  }
  {
    std::string query =
        "GO FROM \"1\" OVER like, serve YIELD like._dst AS id, serve._src AS "
        "id, serve._dst AS DST | GO FROM $-.DST OVER like YIELD $$ as dstNode";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: Duplicate Column Name : `id'");
  }
  {
    std::string query =
        "$a = GO FROM \"1\" OVER * YIELD like._dst AS id, like._src AS id, "
        "serve._dst as DST; "
        "GO FROM $a.DST OVER like YIELD $^ as srcNode";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()), "SemanticError: Duplicate Column Name : `id'");
  }
  {
    std::string query = "GO FROM id(vertex) OVER *  YIELD edge as e";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SemanticError: `id(VERTEX)' is not an evaluable expression.");
  }
  {
    std::string query = "GO FROM \"Tim\" OVER * YIELD vertex as v";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SemanticError: `VERTEX AS v' is not support in go sentence.");
  }
  {
    std::string query = "GO FROM \"Tim\" OVER * YIELD path";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: please add alias when using `path'. near `path'");
  }
  {
    std::string query = "GO FROM \"Tim\" OVER * YIELD $$";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: please add alias when using `$$'. near `$$'");
  }
  {
    std::string query = "GO FROM \"Tim\" OVER * YIELD $^";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SyntaxError: please add alias when using `$^'. near `$^'");
  }
  {
    std::string query = "GO 1 TO 4 STEPS FROM \"Tim\" OVER * YIELD id(vertex) as id";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SemanticError: `id(VERTEX) AS id' is not support in go sentence.");
  }
  {
    std::string query = "GO 2 STEPS FROM \"Tim\" OVER * YIELD vertex as v";
    auto result = checkResult(query);
    EXPECT_EQ(std::string(result.message()),
              "SemanticError: `VERTEX AS v' is not support in go sentence.");
  }
}

TEST_F(QueryValidatorTest, Limit) {
  // Syntax error
  {
    std::string query = "GO FROM \"Ann\" OVER like YIELD like._dst AS like | LIMIT -1, 3";
    EXPECT_FALSE(checkResult(query));
  }
  {
    std::string query = "GO FROM \"Ann\" OVER like YIELD like._dst AS like | LIMIT 1, 3";
    std::vector<PlanNode::Kind> expected = {
        PK::kLimit, PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, OrderBy) {
  {
    std::string query =
        "GO FROM \"Ann\" OVER like YIELD $^.person.age AS age"
        " | ORDER BY $-.age";
    std::vector<PlanNode::Kind> expected = {
        PK::kSort, PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  // not exist factor
  {
    std::string query =
        "GO FROM \"Ann\" OVER like YIELD $^.person.age AS age"
        " | ORDER BY $-.name";
    EXPECT_FALSE(checkResult(query));
  }
}

TEST_F(QueryValidatorTest, OrderByAndLimit) {
  {
    std::string query =
        "GO FROM \"Ann\" OVER like YIELD $^.person.age AS age"
        " | ORDER BY $-.age | LIMIT 1";
    std::vector<PlanNode::Kind> expected = {
        PK::kLimit, PK::kSort, PK::kProject, PK::kExpandAll, PK::kExpand, PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, TestSetValidator) {
  // UNION ALL
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like.start AS start UNION ALL GO FROM "
        "\"2\" OVER like YIELD like.start AS start";
    std::vector<PlanNode::Kind> expected = {PK::kUnion,
                                            PK::kProject,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kStart,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  // UNION DISTINCT twice
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like.start AS start UNION GO FROM \"2\" "
        "OVER like YIELD like.start AS start UNION GO FROM \"3\" OVER like "
        "YIELD like.start AS start";
    std::vector<PlanNode::Kind> expected = {PK::kDedup,
                                            PK::kUnion,
                                            PK::kDedup,
                                            PK::kProject,
                                            PK::kUnion,
                                            PK::kExpandAll,
                                            PK::kProject,
                                            PK::kProject,
                                            PK::kExpand,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kStart,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kStart,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  // UNION DISTINCT
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like.start AS start UNION DISTINCT GO "
        "FROM \"2\" OVER like YIELD like.start AS start";
    std::vector<PlanNode::Kind> expected = {PK::kDedup,
                                            PK::kUnion,
                                            PK::kProject,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kStart,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  // INVALID UNION ALL
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like.start AS start, $^.person.name AS "
        "name UNION GO FROM \"2\" OVER like YIELD like.start AS start";
    EXPECT_FALSE(checkResult(query));
  }
  // INTERSECT
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like.start AS start INTERSECT GO FROM "
        "\"2\" OVER like YIELD like.start AS start";
    std::vector<PlanNode::Kind> expected = {PK::kIntersect,
                                            PK::kProject,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kStart,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
  // MINUS
  {
    std::string query =
        "GO FROM \"1\" OVER like YIELD like.start AS start MINUS GO FROM "
        "\"2\" OVER like YIELD like.start AS start";
    std::vector<PlanNode::Kind> expected = {PK::kMinus,
                                            PK::kProject,
                                            PK::kProject,
                                            PK::kExpandAll,
                                            PK::kExpandAll,
                                            PK::kExpand,
                                            PK::kExpand,
                                            PK::kStart,
                                            PK::kStart};
    EXPECT_TRUE(checkResult(query, expected));
  }
}

TEST_F(QueryValidatorTest, TestMaxAllowedStatements) {
  std::vector<std::string> stmts;
  for (uint32_t i = 0; i < FLAGS_max_allowed_statements; i++) {
    stmts.emplace_back(folly::stringPrintf("CREATE TAG tag_%d(name string)", i));
  }
  auto query = folly::join(";", stmts);
  EXPECT_TRUE(checkResult(query));

  stmts.emplace_back(
      folly::stringPrintf("CREATE TAG tag_%d(name string)", FLAGS_max_allowed_statements));
  query = folly::join(";", stmts);
  auto result = checkResult(query);
  EXPECT_FALSE(result);
  EXPECT_EQ(std::string(result.message()),
            "SemanticError: The maximum number of statements allowed has been "
            "exceeded");
}

TEST_F(QueryValidatorTest, TestMaxAllowedQuerySize) {
  FLAGS_max_allowed_query_size = 256;
  std::string query = "INSERT VERTEX person(name, age) VALUES ";
  std::string value = "\"person_1\":(\"person_1\", 1),";
  int count = (FLAGS_max_allowed_query_size - query.size()) / value.size();
  std::string values;
  values.reserve(FLAGS_max_allowed_query_size);
  for (int i = 0; i < count; ++i) {
    values.append(value);
  }
  values.erase(values.size() - 1);
  query += values;
  EXPECT_TRUE(checkResult(query));
  query.append(",\"person_2\":(\"person_2\", 2);");
  auto result = checkResult(query);
  EXPECT_FALSE(result);
  EXPECT_EQ(std::string(result.message()), "SyntaxError: Query is too large (282 > 256).");
  FLAGS_max_allowed_query_size = 4194304;
}

TEST_F(QueryValidatorTest, TestMatch) {
  {
    std::string query =
        "MATCH (v1:person{name: \"LeBron James\"}) -[r]-> (v2) "
        "RETURN type(r) AS Type, v2.person.name AS Name";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject,
        PK::kProject,
        PK::kAppendVertices,
        PK::kTraverse,
        PK::kIndexScan,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "MATCH (:person{name:'Dwyane Wade'}) -[:like]-> () -[:like]-> (v3) "
        "RETURN DISTINCT v3.person.name AS Name";
    std::vector<PlanNode::Kind> expected = {
        PK::kDedup,
        PK::kProject,
        PK::kProject,
        PK::kAppendVertices,
        PK::kTraverse,
        PK::kTraverse,
        PK::kIndexScan,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "MATCH (v1) -[r]-> (v2) "
        "WHERE id(v1) == \"LeBron James\""
        "RETURN type(r) AS Type, v2.person.name AS Name";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject,
        PK::kProject,
        PK::kAppendVertices,
        PK::kTraverse,
        PK::kDedup,
        PK::kPassThrough,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query =
        "MATCH (v1)-[e:serve*2..3{start_year: 2000}]-(v2) "
        "WHERE id(v1) == \"LeBron James\""
        "RETURN v1, v2";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject,
        PK::kProject,
        PK::kAppendVertices,
        PK::kTraverse,
        PK::kDedup,
        PK::kPassThrough,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
  {
    std::string query = "MATCH p = (n)-[]-(m:person{name:\"LeBron James\"}) RETURN p";
    std::vector<PlanNode::Kind> expected = {
        PK::kProject,
        PK::kProject,
        PK::kAppendVertices,
        PK::kTraverse,
        PK::kIndexScan,
        PK::kStart,
    };
    EXPECT_TRUE(checkResult(query, expected));
  }
}

}  // namespace graph
}  // namespace nebula
