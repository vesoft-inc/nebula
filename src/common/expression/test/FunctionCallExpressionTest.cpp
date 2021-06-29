/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class FunctionCallExpressionTest : public ExpressionTest {};

TEST_F(FunctionCallExpressionTest, FunctionCallTest) {
    {
        TEST_FUNCTION(abs, args_["neg_int"], 1);
        TEST_FUNCTION(abs, args_["neg_float"], 1.1);
        TEST_FUNCTION(abs, args_["int"], 4);
        TEST_FUNCTION(abs, args_["float"], 1.1);
    }
    {
        TEST_FUNCTION(floor, args_["neg_int"], -1);
        TEST_FUNCTION(floor, args_["float"], 1);
        TEST_FUNCTION(floor, args_["neg_float"], -2);
        TEST_FUNCTION(floor, args_["int"], 4);
    }
    {
        TEST_FUNCTION(sqrt, args_["int"], 2);
        TEST_FUNCTION(sqrt, args_["float"], std::sqrt(1.1));
    }
    {
        TEST_FUNCTION(pow, args_["pow"], 8);
        TEST_FUNCTION(exp, args_["int"], std::exp(4));
        TEST_FUNCTION(exp2, args_["int"], 16);

        TEST_FUNCTION(log, args_["int"], std::log(4));
        TEST_FUNCTION(log2, args_["int"], 2);
    }
    {
        TEST_FUNCTION(lower, args_["string"], "abcdefg");
        TEST_FUNCTION(upper, args_["string"], "ABCDEFG");
        TEST_FUNCTION(length, args_["string"], 7);

        TEST_FUNCTION(trim, args_["trim"], "abc");
        TEST_FUNCTION(ltrim, args_["trim"], "abc  ");
        TEST_FUNCTION(rtrim, args_["trim"], " abc");
    }
    {
        TEST_FUNCTION(substr, args_["substr"], "cdef");
        TEST_FUNCTION(left, args_["side"], "abcde");
        TEST_FUNCTION(right, args_["side"], "mnopq");
        TEST_FUNCTION(left, args_["neg_side"], Value::kNullValue);
        TEST_FUNCTION(right, args_["neg_side"], Value::kNullValue);

        TEST_FUNCTION(lpad, args_["pad"], "1231abcdefghijkl");
        TEST_FUNCTION(rpad, args_["pad"], "abcdefghijkl1231");
        TEST_FUNCTION(udf_is_in, args_["udf_is_in"], true);
    }
    {
        // hasSameEdgeInPath
        Path path;
        path.src.vid = "1";
        STEP("2", "edge", 0, 1);
        STEP("1", "edge", 0, -1);
        TEST_FUNCTION(hasSameEdgeInPath, {path}, true);
    }
    {
        // hasSameEdgeInPath
        Path path;
        path.src.vid = "0";
        Step step1, step2, step3;
        STEP("2", "edge", 0, 1);
        STEP("1", "edge", 0, -1);
        STEP("2", "edge", 0, 1);
        TEST_FUNCTION(hasSameEdgeInPath, {path}, true);
    }
    {
        // hasSameEdgeInPath
        Path path;
        path.src.vid = "0";
        Step step1, step2, step3;
        STEP("2", "edge", 0, 1);
        STEP("1", "edge", 0, 1);
        STEP("2", "edge", 0, 1);
        TEST_FUNCTION(hasSameEdgeInPath, {path}, false);
    }
    {
        // hasSameEdgeInPath
        Path path;
        path.src.vid = "0";
        Step step1, step2, step3;
        STEP("2", "edge", 0, 1);
        STEP("1", "edge", 0, -1);
        STEP("2", "edge", 1, 1);
        TEST_FUNCTION(hasSameEdgeInPath, {path}, false);
    }
    // Check function
    {
        auto expr = FunctionCallExpression::make(&pool, "TimE");
        EXPECT_TRUE(expr->isFunc("time"));
        EXPECT_FALSE(expr->isFunc("time_"));
    }
}

TEST_F(FunctionCallExpressionTest, FunctionCallToStringTest) {
    {
        ArgumentList *argList = ArgumentList::make(&pool);
        for (const auto &i : args_["pow"]) {
            argList->addArgument(ConstantExpression::make(&pool, i));
        }
        auto ep = FunctionCallExpression::make(&pool, "pow", argList);
        EXPECT_EQ(ep->toString(), "pow(2,3)");
    }
    {
        ArgumentList *argList = ArgumentList::make(&pool);
        for (const auto &i : args_["udf_is_in"]) {
            argList->addArgument(ConstantExpression::make(&pool, i));
        }
        auto ep = FunctionCallExpression::make(&pool, "udf_is_in", argList);
        EXPECT_EQ(ep->toString(), "udf_is_in(4,1,2,8,4,3,1,0)");
    }
    {
        ArgumentList *argList = ArgumentList::make(&pool);
        for (const auto &i : args_["neg_int"]) {
            argList->addArgument(ConstantExpression::make(&pool, i));
        }
        auto ep = FunctionCallExpression::make(&pool, "abs", argList);
        EXPECT_EQ(ep->toString(), "abs(-1)");
    }
    {
        auto ep = FunctionCallExpression::make(&pool, "now");
        EXPECT_EQ(ep->toString(), "now()");
    }
}
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
