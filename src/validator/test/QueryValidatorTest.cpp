/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/base/Base.h"

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class QueryValidatorTest : public ValidatorTestBase {
public:
    void SetUp() override {
        ValidatorTestBase::SetUp();
        qctx_ = buildContext();
    }

    void TearDown() override {
        ValidatorTestBase::TearDown();
        qctx_.reset();
    }

    StatusOr<ExecutionPlan*> validate(const std::string& query) {
        auto result = GQLParser().parse(query);
        if (!result.ok()) return std::move(result).status();
        auto sentences = std::move(result).value();
        ASTValidator validator(sentences.get(), qctx_.get());
        NG_RETURN_IF_ERROR(validator.validate());
        return qctx_->plan();
    }

protected:
    std::unique_ptr<QueryContext>              qctx_;
};

std::ostream& operator<<(std::ostream& os, const std::vector<PlanNode::Kind>& plan) {
    std::vector<const char*> kinds;
    kinds.reserve(plan.size());
    std::transform(plan.cbegin(), plan.cend(), std::back_inserter(kinds), PlanNode::toString);
    os << "[" << folly::join(", ", kinds) << "]";
    return os;
}

using PK = nebula::graph::PlanNode::Kind;

TEST_F(QueryValidatorTest, Subgraph) {
    std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect,
            PK::kLoop,
            PK::kStart,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
    };
    ASSERT_TRUE(checkResult("GET SUBGRAPH 3 STEPS FROM \"1\"", expected));
}

TEST_F(QueryValidatorTest, TestFirstSentence) {
    auto testFirstSentence = [](StatusOr<ExecutionPlan*> so) -> bool {
        if (so.ok()) return false;
        auto status = std::move(so).status();
        auto err = status.toString();
        return err.find_first_of("SyntaxError: Could not start with the statement") == 0;
    };

    {
        auto status = validate("LIMIT 2, 10");
        ASSERT_TRUE(testFirstSentence(status));
    }
    {
        auto status = validate("LIMIT 2, 10 | YIELD 2");
        ASSERT_TRUE(testFirstSentence(status));
    }
    {
        auto status = validate("LIMIT 2, 10 | YIELD 2 | YIELD 3");
        ASSERT_TRUE(testFirstSentence(status));
    }
    {
        auto status = validate("ORDER BY 1");
        ASSERT_TRUE(testFirstSentence(status));
    }
    {
        auto status = validate("GROUP BY 1");
        ASSERT_TRUE(testFirstSentence(status));
    }
}

TEST_F(QueryValidatorTest, Go) {
    {
        std::string query = "GO FROM \"1\" OVER like";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
    {
        std::string query = "GO 3 STEPS FROM \"1\" OVER like";
        auto status = validate(query);
        // TODO: implement n steps and test plan.
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        std::string query = "GO FROM \"1\" OVER like REVERSELY";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
    {
        std::string query = "GO FROM \"1\" OVER like YIELD like.start";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
    {
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $^.person.name,$^.person.age";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
    {
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $$.person.name,$$.person.age";
        auto status = validate(query);
        // TODO: implement get dst props and test plan.
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER like";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
    {
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER like WHERE like.likeness > 90";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        // TODO
    }
    {
        std::string query = "GO FROM \"1\",\"2\",\"3\" OVER like WHERE $^.person.name == \"me\"";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        // TODO
    }
    {
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id"
                            "| GO FROM $-.id OVER like";
        auto status = validate(query);
        EXPECT_TRUE(status.ok()) << status.status();
        auto plan = std::move(status).value();
        ASSERT_NE(plan, nullptr);
        std::vector<PlanNode::Kind> expected = {
            PK::kProject,
            PK::kGetNeighbors,
            PK::kProject,
            PK::kGetNeighbors,
            PK::kStart,
        };
        ASSERT_TRUE(verifyPlan(plan->root(), expected));
    }
}

TEST_F(QueryValidatorTest, GoInvalid) {
    {
        // friend not exist.
        std::string query = "GO FROM \"1\" OVER friend";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        // manager not exist
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $^.manager.name,$^.person.age";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        // manager not exist
        std::string query = "GO FROM \"1\" OVER like "
                            "YIELD $$.manager.name,$$.person.age";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        // column not exist
        std::string query = "GO FROM \"1\" OVER like YIELD like._dst AS id"
                            "| GO FROM $-.col OVER like";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        // invalid id type
        std::string query = "GO FROM \"1\" OVER like YIELD like.likeness AS id"
                            "| GO FROM $-.id OVER like";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
    {
        // multi inputs
        std::string query = "$var = GO FROM \"2\" OVER like;"
                            "GO FROM \"1\" OVER like YIELD like._dst AS id"
                            "| GO FROM $-.id OVER like WHERE $var.id == \"\"";
        auto status = validate(query);
        EXPECT_FALSE(status.ok()) << status.status();
    }
}

TEST_F(QueryValidatorTest, Limit) {
    // Syntax error
    {
        std::string query = "GO FROM \"Ann\" OVER like YIELD like._dst AS like | LIMIT -1, 3";
        auto status = validate(query);
        ASSERT_FALSE(status.ok()) << status.status();
    }
    {
        std::string query = "GO FROM \"Ann\" OVER like YIELD like._dst AS like | LIMIT 1, 3";
        std::vector<PlanNode::Kind> expected = {
                PK::kDataCollect, PK::kLimit, PK::kProject, PK::kGetNeighbors, PK::kStart
        };
        ASSERT_TRUE(checkResult(query, expected));
    }
}

TEST_F(QueryValidatorTest, OrderBy) {
    {
        std::string query = "GO FROM \"Ann\" OVER like YIELD $^.person.age AS age"
                            " | ORDER BY $-.age";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect, PK::kSort, PK::kProject, PK::kGetNeighbors, PK::kStart
        };
        ASSERT_TRUE(checkResult(query, expected));
    }
    // not exist factor
    {
        std::string query = "GO FROM \"Ann\" OVER like YIELD $^.person.age AS age"
                            " | ORDER BY $-.name";
        auto status = validate(query);
        ASSERT_FALSE(status.ok()) << status.status();
    }
}

TEST_F(QueryValidatorTest, OrderByAndLimt) {
    {
        std::string query = "GO FROM \"Ann\" OVER like YIELD $^.person.age AS age"
                            " | ORDER BY $-.age | LIMIT 1";
        std::vector<PlanNode::Kind> expected = {
            PK::kDataCollect, PK::kLimit, PK::kSort, PK::kProject, PK::kGetNeighbors, PK::kStart
        };
        ASSERT_TRUE(checkResult(query, expected));
    }
}

TEST_F(QueryValidatorTest, TestSetValidator) {
  // UNION ALL
  {
      std::string query =
          "GO FROM \"1\" OVER like YIELD like.start AS start UNION ALL GO FROM \"2\" "
          "OVER like YIELD like.start AS start";
      auto status = validate(query);
      ASSERT_TRUE(status.ok()) << status.status();
      auto plan = std::move(status).value();
      ASSERT_NE(plan, nullptr);
      std::vector<PlanNode::Kind> expected = {
          PK::kDataCollect,
          PK::kUnion,
          PK::kProject,
          PK::kProject,
          PK::kGetNeighbors,
          PK::kGetNeighbors,
          PK::kMultiOutputs,
          PK::kStart,
      };
      ASSERT_TRUE(verifyPlan(plan->root(), expected));
  }
  // UNION DISTINCT twice
  {
      std::string query = "GO FROM \"1\" OVER like YIELD like.start AS start UNION GO FROM \"2\" "
                          "OVER like YIELD like.start AS start UNION GO FROM \"3\" OVER like YIELD "
                          "like.start AS start";
      auto status = validate(query);
      ASSERT_TRUE(status.ok()) << status.status();
      auto plan = std::move(status).value();
      ASSERT_NE(plan, nullptr);
      std::vector<PlanNode::Kind> expected = {
          PK::kDataCollect,
          PK::kDedup,
          PK::kUnion,
          PK::kDedup,
          PK::kProject,
          PK::kUnion,
          PK::kGetNeighbors,
          PK::kProject,
          PK::kProject,
          PK::kMultiOutputs,
          PK::kGetNeighbors,
          PK::kGetNeighbors,
          PK::kStart,
          PK::kMultiOutputs,
      };
      ASSERT_TRUE(verifyPlan(plan->root(), expected));
  }
  // UNION DISTINCT
  {
      std::string query =
          "GO FROM \"1\" OVER like YIELD like.start AS start UNION DISTINCT GO FROM \"2\" "
          "OVER like YIELD like.start AS start";
      auto status = validate(query);
      EXPECT_TRUE(status.ok()) << status.status();
      auto plan = std::move(status).value();
      ASSERT_NE(plan, nullptr);
      std::vector<PlanNode::Kind> expected = {
          PK::kDataCollect,
          PK::kDedup,
          PK::kUnion,
          PK::kProject,
          PK::kProject,
          PK::kGetNeighbors,
          PK::kGetNeighbors,
          PK::kMultiOutputs,
          PK::kStart,
      };
      ASSERT_TRUE(verifyPlan(plan->root(), expected));
  }
  // INVALID UNION ALL
  {
      std::string query = "GO FROM \"1\" OVER like YIELD like.start AS start, $^.person.name AS "
                          "name UNION GO FROM \"2\" OVER like YIELD like.start AS start";
      auto status = validate(query);
      ASSERT_FALSE(status.ok()) << status.status();
  }
  // INTERSECT
  {
      std::string query = "GO FROM \"1\" OVER like YIELD like.start AS start INTERSECT GO FROM "
                          "\"2\" OVER like YIELD like.start AS start";
      auto status = validate(query);
      EXPECT_TRUE(status.ok()) << status.status();
      auto plan = std::move(status).value();
      ASSERT_NE(plan, nullptr);
      std::vector<PlanNode::Kind> expected = {
          PK::kDataCollect,
          PK::kIntersect,
          PK::kProject,
          PK::kProject,
          PK::kGetNeighbors,
          PK::kGetNeighbors,
          PK::kMultiOutputs,
          PK::kStart,
      };
      ASSERT_TRUE(verifyPlan(plan->root(), expected));
  }
  // MINUS
  {
      std::string query = "GO FROM \"1\" OVER like YIELD like.start AS start MINUS GO FROM "
                          "\"2\" OVER like YIELD like.start AS start";
      auto status = validate(query);
      EXPECT_TRUE(status.ok()) << status.status();
      auto plan = std::move(status).value();
      ASSERT_NE(plan, nullptr);
      std::vector<PlanNode::Kind> expected = {
          PK::kDataCollect,
          PK::kMinus,
          PK::kProject,
          PK::kProject,
          PK::kGetNeighbors,
          PK::kGetNeighbors,
          PK::kMultiOutputs,
          PK::kStart,
      };
      ASSERT_TRUE(verifyPlan(plan->root(), expected));
  }
}
}  // namespace graph
}  // namespace nebula
