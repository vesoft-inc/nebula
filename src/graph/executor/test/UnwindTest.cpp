/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "graph/context/QueryContext.h"
#include "graph/executor/query/UnwindExecutor.h"
#include "graph/executor/test/QueryTestBase.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

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
