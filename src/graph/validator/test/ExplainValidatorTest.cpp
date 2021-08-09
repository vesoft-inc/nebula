/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/ExplainValidator.h"

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class ExplainValidatorTest : public ValidatorTestBase {
public:
    void SetUp() override {
        ValidatorTestBase::SetUp();
    }
};

TEST_F(ExplainValidatorTest, TestExplainSingleStmt) {
    {
        std::string query = "EXPLAIN YIELD 1 AS id;";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "EXPLAIN FORMAT=\"dot\" YIELD 1 AS id;";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "EXPLAIN FORMAT=\"row\" YIELD 1 AS id;";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "EXPLAIN FORMAT=\"unknown\" YIELD 1 AS id;";
        EXPECT_FALSE(checkResult(query));
    }
}

TEST_F(ExplainValidatorTest, TestExplainMultiStmts) {
    {
        std::string query = "EXPLAIN {$var=YIELD 1 AS id; YIELD $var.id;};";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "EXPLAIN FORMAT=\"dot\" {$var=YIELD 1 AS id; YIELD $var.id;}";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "EXPLAIN FORMAT=\"row\" {$var=YIELD 1 AS id; YIELD $var.id;}";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "EXPLAIN FORMAT=\"unknown\" {$var=YIELD 1 AS id; YIELD $var.id;}";
        EXPECT_FALSE(checkResult(query));
    }
}

TEST_F(ExplainValidatorTest, TestProfileSingleStmt) {
    {
        std::string query = "PROFILE YIELD 1 AS id;";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "PROFILE FORMAT=\"dot\" YIELD 1 AS id;";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "PROFILE FORMAT=\"row\" YIELD 1 AS id;";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "PROFILE FORMAT=\"unknown\" YIELD 1 AS id;";
        EXPECT_FALSE(checkResult(query));
    }
}

TEST_F(ExplainValidatorTest, TestProfileMultiStmts) {
    {
        std::string query = "PROFILE {$var=YIELD 1 AS id; YIELD $var.id;};";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "PROFILE FORMAT=\"dot\" {$var=YIELD 1 AS id; YIELD $var.id;}";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "PROFILE FORMAT=\"row\" {$var=YIELD 1 AS id; YIELD $var.id;}";
        std::vector<PlanNode::Kind> expected = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected));
    }
    {
        std::string query = "PROFILE FORMAT=\"unknown\" {$var=YIELD 1 AS id; YIELD $var.id;}";
        EXPECT_FALSE(checkResult(query));
    }
}
}   // namespace graph
}   // namespace nebula
