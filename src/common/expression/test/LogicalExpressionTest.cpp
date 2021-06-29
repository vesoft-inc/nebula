/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "common/expression/test/TestBase.h"

namespace nebula {

class LogicalExpressionTest : public ExpressionTest {};

TEST_F(LogicalExpressionTest, LogicalCalculation) {
    {
        TEST_EXPR(true, true);
        TEST_EXPR(false, false);
    }
    {
        TEST_EXPR(true XOR true, false);
        TEST_EXPR(true XOR false, true);
        TEST_EXPR(false XOR false, false);
        TEST_EXPR(false XOR true, true);
    }
    {
        TEST_EXPR(true AND true AND true, true);
        TEST_EXPR(true AND true OR false, true);
        TEST_EXPR(true AND true AND false, false);
        TEST_EXPR(true OR false AND true OR false, true);
        TEST_EXPR(true XOR true XOR false, false);
    }
    {
        // AND
        TEST_EXPR(true AND true, true);
        TEST_EXPR(true AND false, false);
        TEST_EXPR(false AND true, false);
        TEST_EXPR(false AND false, false);

        // AND AND  ===  (AND) AND
        TEST_EXPR(true AND true AND true, true);
        TEST_EXPR(true AND true AND false, false);
        TEST_EXPR(true AND false AND true, false);
        TEST_EXPR(true AND false AND false, false);
        TEST_EXPR(false AND true AND true, false);
        TEST_EXPR(false AND true AND false, false);
        TEST_EXPR(false AND false AND true, false);
        TEST_EXPR(false AND false AND false, false);

        // OR
        TEST_EXPR(true OR true, true);
        TEST_EXPR(true OR false, true);
        TEST_EXPR(false OR true, true);
        TEST_EXPR(false OR false, false);

        // OR OR  ===  (OR) OR
        TEST_EXPR(true OR true OR true, true);
        TEST_EXPR(true OR true OR false, true);
        TEST_EXPR(true OR false OR true, true);
        TEST_EXPR(true OR false OR false, true);
        TEST_EXPR(false OR true OR true, true);
        TEST_EXPR(false OR true OR false, true);
        TEST_EXPR(false OR false OR true, true);
        TEST_EXPR(false OR false OR false, false);

        // AND OR  ===  (AND) OR
        TEST_EXPR(true AND true OR true, true);
        TEST_EXPR(true AND true OR false, true);
        TEST_EXPR(true AND false OR true, true);
        TEST_EXPR(true AND false OR false, false);
        TEST_EXPR(false AND true OR true, true);
        TEST_EXPR(false AND true OR false, false);
        TEST_EXPR(false AND false OR true, true);
        TEST_EXPR(false AND false OR false, false);

        // OR AND  === OR (AND)
        TEST_EXPR(true OR true AND true, true);
        TEST_EXPR(true OR true AND false, true);
        TEST_EXPR(true OR false AND true, true);
        TEST_EXPR(true OR false AND false, true);
        TEST_EXPR(false OR true AND true, true);
        TEST_EXPR(false OR true AND false, false);
        TEST_EXPR(false OR false AND true, false);
        TEST_EXPR(false OR false AND false, false);
    }
    {
        TEST_EXPR(2 > 1 AND 3 > 2, true);
        TEST_EXPR(2 <= 1 AND 3 > 2, false);
        TEST_EXPR(2 > 1 AND 3 < 2, false);
        TEST_EXPR(2 < 1 AND 3 < 2, false);
    }
    {
        // test bad null
        TEST_EXPR(2 / 0, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 AND true, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 AND false, Value::Value::kNullDivByZero);
        TEST_EXPR(true AND 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(false AND 2 / 0, false);
        TEST_EXPR(2 / 0 AND 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(empty AND null AND 2 / 0 AND empty, Value::kNullDivByZero);

        TEST_EXPR(2 / 0 OR true, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 OR false, Value::kNullDivByZero);
        TEST_EXPR(true OR 2 / 0, true);
        TEST_EXPR(false OR 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 OR 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(empty OR null OR 2 / 0 OR empty, Value::kNullDivByZero);

        TEST_EXPR(2 / 0 XOR true, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 XOR false, Value::kNullDivByZero);
        TEST_EXPR(true XOR 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(false XOR 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(2 / 0 XOR 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(empty XOR 2 / 0 XOR null XOR empty, Value::kNullDivByZero);

        // test normal null
        TEST_EXPR(null AND true, Value::kNullValue);
        TEST_EXPR(null AND false, false);
        TEST_EXPR(true AND null, Value::kNullValue);
        TEST_EXPR(false AND null, false);
        TEST_EXPR(null AND null, Value::kNullValue);
        TEST_EXPR(empty AND null AND empty, Value::kNullValue);

        TEST_EXPR(null OR true, true);
        TEST_EXPR(null OR false, Value::kNullValue);
        TEST_EXPR(true OR null, true);
        TEST_EXPR(false OR null, Value::kNullValue);
        TEST_EXPR(null OR null, Value::kNullValue);
        TEST_EXPR(empty OR null OR empty, Value::kNullValue);

        TEST_EXPR(null XOR true, Value::kNullValue);
        TEST_EXPR(null XOR false, Value::kNullValue);
        TEST_EXPR(true XOR null, Value::kNullValue);
        TEST_EXPR(false XOR null, Value::kNullValue);
        TEST_EXPR(null XOR null, Value::kNullValue);
        TEST_EXPR(empty XOR null XOR empty, Value::kNullValue);

        // test empty
        TEST_EXPR(empty, Value::kEmpty);
        TEST_EXPR(empty AND true, Value::kEmpty);
        TEST_EXPR(empty AND false, false);
        TEST_EXPR(true AND empty, Value::kEmpty);
        TEST_EXPR(false AND empty, false);
        TEST_EXPR(empty AND empty, Value::kEmpty);
        TEST_EXPR(empty AND null, Value::kNullValue);
        TEST_EXPR(null AND empty, Value::kNullValue);
        TEST_EXPR(empty AND true AND empty, Value::kEmpty);

        TEST_EXPR(empty OR true, true);
        TEST_EXPR(empty OR false, Value::kEmpty);
        TEST_EXPR(true OR empty, true);
        TEST_EXPR(false OR empty, Value::kEmpty);
        TEST_EXPR(empty OR empty, Value::kEmpty);
        TEST_EXPR(empty OR null, Value::kNullValue);
        TEST_EXPR(null OR empty, Value::kNullValue);
        TEST_EXPR(empty OR false OR empty, Value::kEmpty);

        TEST_EXPR(empty XOR true, Value::kEmpty);
        TEST_EXPR(empty XOR false, Value::kEmpty);
        TEST_EXPR(true XOR empty, Value::kEmpty);
        TEST_EXPR(false XOR empty, Value::kEmpty);
        TEST_EXPR(empty XOR empty, Value::kEmpty);
        TEST_EXPR(empty XOR null, Value::kNullValue);
        TEST_EXPR(null XOR empty, Value::kNullValue);
        TEST_EXPR(true XOR empty XOR false, Value::kEmpty);

        TEST_EXPR(empty OR false AND true AND null XOR empty, Value::kEmpty);
        TEST_EXPR(empty OR false AND true XOR empty OR true, true);

        TEST_EXPR((empty OR false) AND true XOR empty XOR null AND 2 / 0, Value::kNullValue);
        // empty OR false AND 2/0
        TEST_EXPR(empty OR false AND true XOR empty XOR null AND 2 / 0, Value::kEmpty);
        TEST_EXPR(empty AND true XOR empty XOR null AND 2 / 0, Value::kNullValue);
        TEST_EXPR(empty OR false AND true XOR empty OR null AND 2 / 0, Value::kNullDivByZero);
        TEST_EXPR(empty OR false AND empty XOR empty OR null, Value::kNullValue);
    }
}
}   // namespace nebula

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);

    return RUN_ALL_TESTS();
}
