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

TEST_F(YieldValidatorTest, HashCall) {
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
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `!(0)' is not a valid expression, can not apply `!' to INT.");
    }
}

TEST_F(YieldValidatorTest, Logic) {
    {
        std::string query = "YIELD NOT TRUE OR !FALSE";
        EXPECT_TRUE(checkResult(query, expected_));
    }
#if 0
    {
        std::string query = "YIELD NOT 0 OR 0 AND 0 XOR 0";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD !0 OR 0 AND 0 XOR 1";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (NOT 0 OR 0) AND 0 XOR 1";
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

TEST_F(YieldValidatorTest, FuncitonCall) {
#if 0
    {
        // TODO not support udf_is_in
        std::string query = "YIELD udf_is_in(1,0,1,2), 123";
        EXPECT_TRUE(checkResult(query, expected_));
    }
#endif
    {
        std::string query = "YIELD abs(-12)";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD abs(0)";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD abs(true)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `abs(true)' is not a valid expression : Parameter's type error");
    }
    {
        std::string query = "YIELD abs(\"test\")";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `abs(\"test\")' is not a valid expression : "
                  "Parameter's type error");
    }
    {
        // TODO: move to parser UT
        std::string query = "YIELD noexist(12)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SyntaxError: Unknown function  near `noexist'");
    }
}

TEST_F(YieldValidatorTest, TypeCastTest) {
    {
        std::string query = "YIELD (INT)1.23";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (INT).123";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (INT)123";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (INT)\"  123\"";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (int64)-123";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (int32)\"-123\"";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (int30)(\"-123\")";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SyntaxError: syntax error near `(\"-123\")'");
    }
    {
        std::string query = "YIELD (int)\"123abc\"";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `(INT)\"123abc\"' is not a valid expression ");
    }
    {
        std::string query = "YIELD (int)\"abc123\"";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `(INT)\"abc123\"' is not a valid expression ");
    }
    {
        std::string query = "YIELD (doublE)\"123\"";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (doublE)\"  123\"";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (doublE)\".a123\"";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: `(FLOAT)\".a123\"' is not a valid expression ");
    }
    {
        std::string query = "YIELD (STRING)1.23";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (STRING)123";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (STRING)true";
        EXPECT_TRUE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (BOOL)123";
        EXPECT_FALSE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (BOOL)0";
        EXPECT_FALSE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (BOOL)\"12\"";
        EXPECT_FALSE(checkResult(query, expected_));
    }
    {
        std::string query = "YIELD (MAP)(\"12\")";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SyntaxError: syntax error near `(\"12\")'");
    }
    {
        std::string query = "YIELD (SET)12";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SyntaxError: syntax error near `SET'");
    }
    {
        std::string query = "YIELD (PATH)true";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SyntaxError: syntax error near `true'");
    }
    {
        std::string query = "YIELD (NOEXIST)true";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()), "SyntaxError: syntax error near `true'");
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
        EXPECT_TRUE(checkResult(query, expected_, {"$-.name", "$-.start"}));
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
        EXPECT_TRUE(checkResult(query, expected_, {"$var.name", "$var.start"}));
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
        EXPECT_TRUE(checkResult(query, expected_, {"$var.name", "$var.start"}));
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
        EXPECT_TRUE(checkResult(query, expected_, {"$var.name", "$var.start", "hash"}));
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
        EXPECT_TRUE(checkResult(query, expected_, {"$var.name", "$var.start", "hash"}));
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
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Not support both input and variable.");
    }
    {
        // Not support reference two different variable
        auto query = var + "YIELD $var.name WHERE $var1.start > 2005";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Only one variable allowed to use.");
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
                  "SemanticError: Only support input and variable in yield sentence.");
    }
    {
        auto query = var + "YIELD $^.person.name";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Only support input and variable in yield sentence.");
    }
    {
        auto query = var + "YIELD like.start";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Invalid label identifiers: like");
    }
}

TEST_F(YieldValidatorTest, AggCall) {
    {
        std::string query = "YIELD COUNT(1), $-.name";
        auto result = checkResult(query);
        // Error would be reported when no input
        // EXPECT_EQ(std::string(result.message()), "SemanticError: column `name' not exist in
        // input");
        EXPECT_EQ(std::string(result.message()), "SemanticError: `$-.name', not exist prop `name'");
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
        EXPECT_TRUE(checkResult(query));
    }
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
        EXPECT_TRUE(checkResult(query));
    }
    {
        auto query = "GO FROM \"1\" OVER like "
                     "YIELD $^.person.age AS age, "
                     "like.likeness AS like"
                     "| YIELD AVG($-.*)";
        auto result = checkResult(query);
        EXPECT_EQ(std::string(result.message()),
                  "SemanticError: Could not apply aggregation function `AVG($-.*)' on `$-.*'");
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
        EXPECT_TRUE(checkResult(query));
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
                  "SemanticError: Could not apply aggregation function `AVG($var.*)' on `$var.*'");
    }
}

}   // namespace graph
}   // namespace nebula
