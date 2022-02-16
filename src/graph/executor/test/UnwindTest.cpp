/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/futures/Future.h>  // for Future::get
#include <gtest/gtest.h>           // for Message
#include <gtest/gtest.h>           // for TestPartResult
#include <gtest/gtest.h>           // for Message
#include <gtest/gtest.h>           // for TestPartResult

#include <memory>         // for allocator, unique_ptr
#include <string>         // for string, basic_string
#include <type_traits>    // for remove_reference<>...
#include <unordered_map>  // for unordered_map
#include <utility>        // for move
#include <vector>         // for vector

#include "common/base/Status.h"                    // for Status
#include "common/datatypes/DataSet.h"              // for Row, DataSet
#include "common/datatypes/List.h"                 // for List
#include "common/datatypes/Value.h"                // for Value, NullType
#include "common/expression/ConstantExpression.h"  // for ConstantExpression
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/QueryContext.h"            // for QueryContext
#include "graph/context/Result.h"                  // for Result, Result::State
#include "graph/executor/Executor.h"               // for Executor
#include "graph/executor/test/QueryTestBase.h"     // for QueryTestBase
#include "graph/planner/plan/Logic.h"              // for StartNode
#include "graph/planner/plan/Query.h"              // for Unwind

namespace nebula {
class ObjectPool;
namespace graph {
class UnwindTest_UnwindList_Test;
}  // namespace graph

class ObjectPool;

namespace graph {
class UnwindTest_UnwindList_Test;

class UnwindTest : public QueryTestBase {
 protected:
  void SetUp() override {
    qctx_ = std::make_unique<QueryContext>();
    pool_ = qctx_->objPool();
    start_ = StartNode::make(qctx_.get());
  }

  void testUnwind(std::vector<Value> l) {
    auto list = ConstantExpression::make(pool_, List(l));
    auto* unwind = Unwind::make(qctx_.get(), start_, list, "items");
    unwind->setColNames(std::vector<std::string>{"items"});

    auto unwExe = Executor::create(unwind, qctx_.get());
    auto future = unwExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(unwind->outputVar());

    DataSet expected;
    expected.colNames = {"items"};
    for (auto v : l) {
      Row row;
      row.values.emplace_back(v);
      expected.rows.emplace_back(std::move(row));
    }
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
  }

 protected:
  std::unique_ptr<QueryContext> qctx_;
  ObjectPool* pool_;
  StartNode* start_;
};

#define TEST_UNWIND(list) \
  do {                    \
    testUnwind(list);     \
  } while (0)

static std::unordered_map<std::string, std::vector<Value>> testSuite = {
    {"case1", {1, 2, 3}},
    {"case2", {1, List({2, Value((NullType::__NULL__)), 3}), 4, Value((NullType::__NULL__))}},
};

TEST_F(UnwindTest, UnwindList) {
  TEST_UNWIND(testSuite["case1"]);
  TEST_UNWIND(testSuite["case2"]);
}

}  // namespace graph
}  // namespace nebula
