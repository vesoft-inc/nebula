/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "graph/context/ExecutionContext.h"
#include "graph/context/QueryExpressionContext.h"

namespace nebula {
namespace graph {
TEST(ExpressionContextTest, GetVar) {
  ExecutionContext ectx;
  ectx.setValue("v1", 10);
  ectx.setValue("v2", "Hello world");

  graph::QueryExpressionContext qECtx(&ectx);
  EXPECT_EQ(Value(10), qECtx(nullptr).getVar("v1"));
  EXPECT_EQ(Value("Hello world"), qECtx(nullptr).getVar("v2"));

  ectx.setValue("v1", "Hello world");
  ectx.setValue("v1", 3.14);
  ectx.setValue("v1", true);
  EXPECT_EQ(Value(true), qECtx(nullptr).getVersionedVar("v1", 0));
  EXPECT_EQ(Value(3.14), qECtx(nullptr).getVersionedVar("v1", -1));
  EXPECT_EQ(Value(10), qECtx(nullptr).getVersionedVar("v1", 1));
}
}  // namespace graph
}  // namespace nebula
