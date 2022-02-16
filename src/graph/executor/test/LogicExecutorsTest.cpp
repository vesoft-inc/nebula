/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/futures/Future.h>  // for Future::get
#include <gtest/gtest.h>           // for Message
#include <gtest/gtest.h>           // for TestPartResult
#include <gtest/gtest.h>           // for Message
#include <gtest/gtest.h>           // for TestPartResult
#include <stddef.h>                // for size_t
#include <stdint.h>                // for int32_t

#include <memory>       // for unique_ptr, allo...
#include <string>       // for string
#include <type_traits>  // for remove_reference...
#include <utility>      // for move

#include "common/base/Status.h"                      // for Status
#include "common/datatypes/Value.h"                  // for Value
#include "common/expression/ConstantExpression.h"    // for ConstantExpression
#include "common/expression/RelationalExpression.h"  // for RelationalExpres...
#include "common/expression/UnaryExpression.h"       // for UnaryExpression
#include "common/expression/VariableExpression.h"    // for VersionedVariabl...
#include "graph/context/ExecutionContext.h"          // for ExecutionContext
#include "graph/context/QueryContext.h"              // for QueryContext
#include "graph/context/Result.h"                    // for Result
#include "graph/executor/Executor.h"                 // for Executor
#include "graph/executor/logic/StartExecutor.h"      // for StartExecutor
#include "graph/planner/plan/Logic.h"                // for StartNode, Select

namespace nebula {
class ObjectPool;

class ObjectPool;

namespace graph {
class LogicExecutorsTest : public testing::Test {
 protected:
  void SetUp() override {
    qctx_ = std::make_unique<QueryContext>();
    pool_ = qctx_->objPool();
  }

 protected:
  std::unique_ptr<QueryContext> qctx_;
  ObjectPool* pool_;
};

TEST_F(LogicExecutorsTest, Start) {
  auto* start = StartNode::make(qctx_.get());
  auto startExe = std::make_unique<StartExecutor>(start, qctx_.get());
  auto f = startExe->execute();
  auto status = std::move(f).get();
  EXPECT_TRUE(status.ok());
}

TEST_F(LogicExecutorsTest, Loop) {
  std::string counter = "counter";
  qctx_->ectx()->setValue(counter, 0);
  // ++counter{0} <= 5
  auto condition = RelationalExpression::makeLE(
      pool_,
      UnaryExpression::makeIncr(
          pool_,
          VersionedVariableExpression::make(pool_, counter, ConstantExpression::make(pool_, 0))),
      ConstantExpression::make(pool_, static_cast<int32_t>(5)));
  auto* start = StartNode::make(qctx_.get());
  auto* loop = Loop::make(qctx_.get(), start, start, condition);
  auto loopExe = Executor::create(loop, qctx_.get());
  for (size_t i = 0; i < 5; ++i) {
    auto f = loopExe->execute();
    auto status = std::move(f).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(loop->outputVar());
    auto& value = result.value();
    EXPECT_TRUE(value.isBool());
    EXPECT_TRUE(value.getBool());
  }

  auto f = loopExe->execute();
  auto status = std::move(f).get();
  EXPECT_TRUE(status.ok());
  auto& result = qctx_->ectx()->getResult(loop->outputVar());
  auto& value = result.value();
  EXPECT_TRUE(value.isBool());
  EXPECT_FALSE(value.getBool());
}

TEST_F(LogicExecutorsTest, Select) {
  {
    auto* start = StartNode::make(qctx_.get());
    auto condition = ConstantExpression::make(pool_, true);
    auto* select = Select::make(qctx_.get(), start, start, start, condition);

    auto selectExe = Executor::create(select, qctx_.get());

    auto f = selectExe->execute();
    auto status = std::move(f).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(select->outputVar());
    auto& value = result.value();
    EXPECT_TRUE(value.isBool());
    EXPECT_TRUE(value.getBool());
  }
  {
    auto* start = StartNode::make(qctx_.get());
    auto condition = ConstantExpression::make(pool_, false);
    auto* select = Select::make(qctx_.get(), start, start, start, condition);

    auto selectExe = Executor::create(select, qctx_.get());

    auto f = selectExe->execute();
    auto status = std::move(f).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(select->outputVar());
    auto& value = result.value();
    EXPECT_TRUE(value.isBool());
    EXPECT_FALSE(value.getBool());
  }
}
}  // namespace graph
}  // namespace nebula
