/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "graph/visitor/DeduceAliasTypeVisitor.h"
#include "graph/visitor/test/VisitorTestBase.h"

namespace nebula {
namespace graph {
class DeduceAliasTypeVisitorTest : public VisitorTestBase {};

TEST_F(DeduceAliasTypeVisitorTest, SubscriptExpr) {
  {
    auto* expr = VertexExpression::make(pool);
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kRuntime);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kNode);
  }
  {
    auto* expr = EdgeExpression::make(pool);
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kRuntime);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kEdge);
  }
  {
    auto* expr = PathBuildExpression::make(pool);
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kRuntime);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kPath);
  }
  // FunctionCallExpression
  {
    auto* expr = FunctionCallExpression::make(pool, "nodes");
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kRuntime);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kNodeList);
  }
  {
    auto* expr = FunctionCallExpression::make(pool, "relationships");
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kRuntime);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kEdgeList);
  }
  {
    auto* expr = FunctionCallExpression::make(pool, "reversepath");
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kRuntime);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kPath);
  }
  {
    auto* expr = FunctionCallExpression::make(pool, "startnode");
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kRuntime);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kNode);
  }
  {
    auto* expr = FunctionCallExpression::make(pool, "endnode");
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kRuntime);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kNode);
  }

  // SubscriptExpression
  {
    auto* items = ExpressionList::make(pool);
    auto expr = SubscriptExpression::make(
        pool, ListExpression::make(pool, items), ConstantExpression::make(pool, 1));
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kRuntime);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kRuntime);
  }
  {
    auto expr = SubscriptExpression::make(pool,
                                          FunctionCallExpression::make(pool, "relationships"),
                                          ConstantExpression::make(pool, 1));
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kEdgeList);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kEdge);
  }
  {
    auto expr = SubscriptExpression::make(
        pool, FunctionCallExpression::make(pool, "nodes"), ConstantExpression::make(pool, 1));
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kNodeList);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kNode);
  }

  // SubscriptRangeExpression
  {
    auto* items = ExpressionList::make(pool);
    auto expr = SubscriptRangeExpression::make(
        pool, ListExpression::make(pool, items), ConstantExpression::make(pool, 1));
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kRuntime);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kRuntime);
  }
  {
    auto expr = SubscriptRangeExpression::make(pool,
                                               FunctionCallExpression::make(pool, "relationships"),
                                               ConstantExpression::make(pool, 1));
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kEdgeList);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kEdgeList);
  }
  {
    auto expr = SubscriptRangeExpression::make(
        pool, FunctionCallExpression::make(pool, "nodes"), ConstantExpression::make(pool, 1));
    DeduceAliasTypeVisitor visitor(nullptr, nullptr, 0, AliasType::kNodeList);
    expr->accept(&visitor);
    EXPECT_TRUE(visitor.ok());
    EXPECT_EQ(visitor.outputType(), AliasType::kNodeList);
  }
}

}  // namespace graph
}  // namespace nebula
