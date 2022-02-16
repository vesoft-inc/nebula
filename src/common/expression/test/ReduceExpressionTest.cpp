/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartRe...
#include <gtest/gtest.h>  // for Message
#include <gtest/gtest.h>  // for TestPartRe...

#include <memory>  // for allocator

#include "common/datatypes/Value.h"                        // for Value, Val...
#include "common/expression/ArithmeticExpression.h"        // for Arithmetic...
#include "common/expression/ConstantExpression.h"          // for ConstantEx...
#include "common/expression/Expression.h"                  // for Expression
#include "common/expression/FunctionCallExpression.h"      // for ArgumentList
#include "common/expression/LabelExpression.h"             // for LabelExpre...
#include "common/expression/ReduceExpression.h"            // for ReduceExpr...
#include "common/expression/VariableExpression.h"          // for VariableEx...
#include "common/expression/test/ExpressionContextMock.h"  // for Expression...
#include "common/expression/test/TestBase.h"               // for pool, Expr...

namespace nebula {

class ReduceExpressionTest : public ExpressionTest {};

TEST_F(ReduceExpressionTest, ReduceEvaluate) {
  {
    // reduce(totalNum = 2 * 10, n IN range(1, 5) | totalNum + n * 2)
    ArgumentList *argList = ArgumentList::make(&pool);
    argList->addArgument(ConstantExpression::make(&pool, 1));
    argList->addArgument(ConstantExpression::make(&pool, 5));
    auto expr = ReduceExpression::make(
        &pool,
        "totalNum",
        ArithmeticExpression::makeMultiply(
            &pool, ConstantExpression::make(&pool, 2), ConstantExpression::make(&pool, 10)),
        "n",
        FunctionCallExpression::make(&pool, "range", argList),
        ArithmeticExpression::makeAdd(
            &pool,
            VariableExpression::make(&pool, "totalNum"),
            ArithmeticExpression::makeMultiply(
                &pool, VariableExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2))));

    auto value = Expression::eval(expr, gExpCtxt);
    ASSERT_EQ(Value::Type::INT, value.type());
    ASSERT_EQ(50, value.getInt());
  }
}

TEST_F(ReduceExpressionTest, ReduceExprToString) {
  {
    // reduce(totalNum = 2 * 10, n IN range(1, 5) | totalNum + n * 2)
    ArgumentList *argList = ArgumentList::make(&pool);
    argList->addArgument(ConstantExpression::make(&pool, 1));
    argList->addArgument(ConstantExpression::make(&pool, 5));
    auto expr = ReduceExpression::make(
        &pool,
        "totalNum",
        ArithmeticExpression::makeMultiply(
            &pool, ConstantExpression::make(&pool, 2), ConstantExpression::make(&pool, 10)),
        "n",
        FunctionCallExpression::make(&pool, "range", argList),
        ArithmeticExpression::makeAdd(
            &pool,
            LabelExpression::make(&pool, "totalNum"),
            ArithmeticExpression::makeMultiply(
                &pool, LabelExpression::make(&pool, "n"), ConstantExpression::make(&pool, 2))));
    ASSERT_EQ("reduce(totalNum = (2*10), n IN range(1,5) | (totalNum+(n*2)))", expr->toString());
  }
}
}  // namespace nebula
