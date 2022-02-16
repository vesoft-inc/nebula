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
#include "common/expression/Expression.h"                  // for Expression
#include "common/expression/PropertyExpression.h"          // for DestProper...
#include "common/expression/test/ExpressionContextMock.h"  // for Expression...
#include "common/expression/test/TestBase.h"               // for pool, gExp...

namespace nebula {

class PropertyExpressionTest : public ExpressionTest {};

TEST_F(PropertyExpressionTest, EdgeTest) {
  {
    // EdgeName._src
    auto ep = EdgeSrcIdExpression::make(&pool, "edge1");
    auto eval = Expression::eval(ep, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 1);
  }
  {
    // EdgeName._type
    auto ep = EdgeTypeExpression::make(&pool, "edge1");
    auto eval = Expression::eval(ep, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 1);
  }
  {
    // EdgeName._rank
    auto ep = EdgeRankExpression::make(&pool, "edge1");
    auto eval = Expression::eval(ep, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 1);
  }
  {
    // EdgeName._dst
    auto ep = EdgeDstIdExpression::make(&pool, "edge1");
    auto eval = Expression::eval(ep, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 1);
  }
}

TEST_F(PropertyExpressionTest, GetProp) {
  {
    // e1.int
    auto ep = EdgePropertyExpression::make(&pool, "e1", "int");
    auto eval = Expression::eval(ep, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 1);
  }
  {
    // t1.int
    auto ep = TagPropertyExpression::make(&pool, "t1", "int");
    auto eval = Expression::eval(ep, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 1);
  }
  {
    // $-.int
    auto ep = InputPropertyExpression::make(&pool, "int");
    auto eval = Expression::eval(ep, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 1);
  }
  {
    // $^.source.int
    auto ep = SourcePropertyExpression::make(&pool, "source", "int");
    auto eval = Expression::eval(ep, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 1);
  }
  {
    // $$.dest.int
    auto ep = DestPropertyExpression::make(&pool, "dest", "int");
    auto eval = Expression::eval(ep, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::INT);
    EXPECT_EQ(eval, 1);
  }
  {
    // $var.float
    auto ep = VariablePropertyExpression::make(&pool, "var", "float");
    auto eval = Expression::eval(ep, gExpCtxt);
    EXPECT_EQ(eval.type(), Value::Type::FLOAT);
    EXPECT_EQ(eval, 1.1);
  }
}

TEST_F(PropertyExpressionTest, PropertyToStringTest) {
  {
    auto ep = DestPropertyExpression::make(&pool, "like", "likeness");
    EXPECT_EQ(ep->toString(), "$$.like.likeness");
  }
  {
    auto ep = EdgePropertyExpression::make(&pool, "like", "likeness");
    EXPECT_EQ(ep->toString(), "like.likeness");
  }
  {
    auto ep = InputPropertyExpression::make(&pool, "name");
    EXPECT_EQ(ep->toString(), "$-.name");
  }
  {
    auto ep = VariablePropertyExpression::make(&pool, "player", "name");
    EXPECT_EQ(ep->toString(), "$player.name");
  }
  {
    auto ep = SourcePropertyExpression::make(&pool, "player", "age");
    EXPECT_EQ(ep->toString(), "$^.player.age");
  }
  {
    auto ep = DestPropertyExpression::make(&pool, "player", "age");
    EXPECT_EQ(ep->toString(), "$$.player.age");
  }
  {
    auto ep = EdgeSrcIdExpression::make(&pool, "like");
    EXPECT_EQ(ep->toString(), "like._src");
  }
  {
    auto ep = EdgeTypeExpression::make(&pool, "like");
    EXPECT_EQ(ep->toString(), "like._type");
  }
  {
    auto ep = EdgeRankExpression::make(&pool, "like");
    EXPECT_EQ(ep->toString(), "like._rank");
  }
  {
    auto ep = EdgeDstIdExpression::make(&pool, "like");
    EXPECT_EQ(ep->toString(), "like._dst");
  }
}
}  // namespace nebula
