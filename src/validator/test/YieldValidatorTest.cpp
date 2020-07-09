/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/YieldValidator.h"

#include "validator/test/ValidatorTestBase.h"

namespace nebula {
namespace graph {

class YieldValidatorTest : public ValidatorTestBase {
public:
    void SetUp() override {
        ValidatorTestBase::SetUp();
        expected_ = {PlanNode::Kind::kProject, PlanNode::Kind::kStart};
    }

protected:
    std::vector<PlanNode::Kind> expected_;
};

TEST_F(YieldValidatorTest, Base) {
    {
        std::string query = "YIELD 1";
        EXPECT_TRUE(checkResult(query, expected_));
    }
#if 0
    {
        std::string query = "YIELD 1+1, '1+1', (int)3.14, (string)(1+1), (string)true";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD \"Hello\", hash(\"Hello\")";
        EXPECT_TRUE(checkResult(query, expected_));
    }
#endif
}

TEST_F(YieldValidatorTest, DISABLED_HashCall) {
    {
        std::string query = "YIELD hash(\"Boris\")";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD hash(123)";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD hash(123 + 456)";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD hash(123.0)";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD hash(!0)";
        EXPECT_TRUE(checkResult(query, expected_));
    }
}

TEST_F(YieldValidatorTest, Logic) {
    {
        std::string query = "YIELD NOT TRUE || !FALSE";
        EXPECT_TRUE(checkResult(query, expected_));
    }
#if 0
    {
        std::string query = "YIELD NOT 0 || 0 AND 0 XOR 0";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD !0 OR 0 && 0 XOR 1";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (NOT 0 || 0) AND 0 XOR 1";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD 2.5 % 1.2 ^ 1.6";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (5 % 3) ^ 1";
        EXPECT_TRUE(checkResult(query, expected_));
    }
#endif
}

TEST_F(YieldValidatorTest, DISABLED_InCall) {
    {
        std::string query = "YIELD udf_is_in(1,0,1,2), 123";
        EXPECT_TRUE(checkResult(query, expected_));
    }
}

TEST_F(YieldValidatorTest, YieldPipe) {
    std::string go = "GO FROM \"1\" OVER like YIELD "
                     "$^.person.name as name, like.start as start";
    {
        auto query = go + "| YIELD $-.start";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto query = go + "| YIELD $-.start WHERE 1 == 1";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto query = go + "| YIELD $-.start WHERE $-.start > 2005";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto query = go + "| YIELD $-.*";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto query = go + "| YIELD $-.* WHERE $-.start > 2005";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start"}));
    }
#if 0
    {
        auto query = go + "| YIELD $-.*, hash(123) as hash WHERE $-.start > 2005";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start", "hash"}));
    }
    {
        auto query = go + "| YIELD DISTINCT $-.*, hash(123) as hash WHERE $-.start > 2005";
        expected_ = {
            PlanNode::Kind::kDataCollect,
            PlanNode::Kind::kDedup,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start", "hash"}));
    }
    {
        auto query = go + "| YIELD DISTINCT hash($-.*) as hash WHERE $-.start > 2005";
        EXPECT_FALSE(checkResult(query));
    }
#endif
    {
        auto query = go + "| YIELD DISTINCT 1 + $-.* AS e WHERE $-.start > 2005";
        EXPECT_FALSE(checkResult(query));
    }
}

TEST_F(YieldValidatorTest, YieldVar) {
    std::string var = " $var = GO FROM \"1\" OVER like YIELD "
                      "$^.person.name as name, like.start as start;";
    {
        auto query = var + "YIELD $var.name";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"$var.name"}));
    }
    {
        auto query = var + "YIELD $var.name WHERE 1 == 1";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"$var.name"}));
    }
    {
        auto query = var + "YIELD $var.name WHERE $var.start > 2005";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto query = var + "YIELD $var.*";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start"}));
    }
    {
        auto query = var + "YIELD $var.* WHERE $var.start > 2005";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start"}));
    }
#if 0
    {
        auto query = var + "YIELD $var.*, hash(123) as hash WHERE $var.start > 2005";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start", "hash"}));
    }
#endif
    {
        auto query = var + "YIELD 2 + $var.* AS e WHERE $var.start > 2005";
        EXPECT_FALSE(checkResult(query));
    }
#if 0
    {
        auto query = var + "YIELD DISTINCT $var.*, hash(123) as hash WHERE $var.start > 2005";
        expected_ = {
            PlanNode::Kind::kDataCollect,
            PlanNode::Kind::kDedup,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kFilter,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_, {"name", "start", "hash"}));
    }
#endif
}

TEST_F(YieldValidatorTest, Error) {
    {
        // Reference input in a single yield sentence is meaningless.
        auto query = "yield $-";
        EXPECT_FALSE(checkResult(query));
    }
    std::string var = " $var = GO FROM \"1\" OVER like YIELD "
                      "$^.person.name AS name, like.start AS start;";
    {
        // Not support reference input and variable
        auto query = var + "YIELD $var.name WHERE $-.start > 2005";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "Not support both input and variable.");
    }
    {
        // Not support reference two different variable
        auto query = var + "YIELD $var.name WHERE $var1.start > 2005";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "Only one variable allowed to use.");
    }
    {
        // Reference a non-existed prop is meaningless.
        auto query = var + "YIELD $var.abc";
        EXPECT_FALSE(checkResult(query));
    }
    {
        // Reference a non-existed prop is meaningless.
        auto query = "GO FROM \"%s\" OVER like | YIELD $-.abc;";
        EXPECT_FALSE(checkResult(query));
    }
    {
        // Reference properties in single yield sentence is meaningless.
        auto query = var + "YIELD $$.person.name";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: Only support input and variable in yield sentence.");
    }
    {
        auto query = var + "YIELD $^.person.name";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: Only support input and variable in yield sentence.");
    }
    {
        auto query = var + "YIELD like.start";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: Only support input and variable in yield sentence.");
    }
}

TEST_F(YieldValidatorTest, AggCall) {
    {
        std::string query = "YIELD COUNT(1), $-.name";
        auto result = checkResult(query);
        // Error would be reported when no input
        // EXPECT_EQ(std::string(result.message()), "SyntaxError: column `name' not exist in
        // input");
        EXPECT_EQ(std::string(result.message()), "`$-.name', not exist prop `name'");
    }
    {
        std::string query = "YIELD COUNT(*), 1+1";
        EXPECT_TRUE(checkResult(query));
    }
    {
        auto query = "GO FROM \"1\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like"
                     "| YIELD COUNT(*), $-.age";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: Input columns without aggregation are not supported in YIELD "
                  "statement without GROUP BY, near `$-.age'");
    }
    // Test input
    {
        auto query = "GO FROM \"1\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like"
                     "| YIELD AVG($-.age), SUM($-.like), COUNT(*), 1+1";
        expected_ = {
            PlanNode::Kind::kAggregate,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto query = "GO FROM \"1\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like"
                     "| YIELD AVG($-.age + 1), SUM($-.like), COUNT(*)";
        expected_ = {
            PlanNode::Kind::kAggregate,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto query = "GO FROM \"1\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like"
                     "| YIELD DISTINCT AVG($-.age + 1), SUM($-.like), COUNT(*)";
        expected_ = {
            PlanNode::Kind::kDataCollect,
            PlanNode::Kind::kDedup,
            PlanNode::Kind::kAggregate,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto query = "GO FROM \"1\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like"
                     "| YIELD AVG($-.age), SUM($-.like), COUNT(*), $-.age + 1";
        EXPECT_FALSE(checkResult(query));
    }
    {
        auto query = "GO FROM \"1\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like"
                     "| YIELD AVG($-.*)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: could not apply aggregation function on `$-.*'");
    }
    // Yield field has not input
    {
        auto query = "GO FROM \"1\" OVER like | YIELD COUNT(*)";
        expected_ = {
            PlanNode::Kind::kAggregate,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    // Yield field has not input
    {
        auto query = "GO FROM \"1\" OVER like | YIELD 1";
        expected_ = {
            PlanNode::Kind::kProject,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    // Test var
    {
        auto query = "$var = GO FROM \"1\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like;"
                     "YIELD AVG($var.age), SUM($var.like), COUNT(*)";
        expected_ = {
            PlanNode::Kind::kAggregate,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        auto query = "$var = GO FROM \"1\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like;"
                     "YIELD AVG($var.age), SUM($var.like), COUNT(*), $var.age + 1";
        EXPECT_FALSE(checkResult(query));
    }
    {
        auto query = "$var = GO FROM \"1\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like;"
                     "YIELD AVG($var.age + 1), SUM($var.like), COUNT(*)";
        expected_ = {
            PlanNode::Kind::kAggregate,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query));
    }
    {
        auto query = "$var = GO FROM \"1\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like;"
                     "YIELD DISTINCT AVG($var.age + 1), SUM($var.like), COUNT(*)";
        expected_ = {
            PlanNode::Kind::kDataCollect,
            PlanNode::Kind::kDedup,
            PlanNode::Kind::kAggregate,
            PlanNode::Kind::kProject,
            PlanNode::Kind::kGetNeighbors,
            PlanNode::Kind::kStart,
        };
        EXPECT_TRUE(checkResult(query));
    }
    {
        auto query = "$var = GO FROM \"%s\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like;"
                     "YIELD AVG($var.*)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: could not apply aggregation function on `$var.*'");
    }
}

}   // namespace graph
}   // namespace nebula
