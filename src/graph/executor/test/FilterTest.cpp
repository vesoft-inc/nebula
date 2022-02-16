/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#include <folly/Try.h>              // for Try::throwUnlessValue
#include <folly/futures/Future.h>   // for Future::get, Futur...
#include <folly/futures/Future.h>   // for Future
#include <folly/futures/Future.h>   // for Future::get, Futur...
#include <folly/futures/Future.h>   // for Future
#include <folly/futures/Promise.h>  // for PromiseException::...
#include <gtest/gtest.h>            // for Message
#include <gtest/gtest.h>            // for TestPartResult
#include <gtest/gtest.h>            // for Message
#include <gtest/gtest.h>            // for TestPartResult

#include <memory>  // for unique_ptr, allocator
#include <string>  // for string, basic_string
#include <vector>  // for vector

#include "common/base/Status.h"                    // for Status
#include "common/datatypes/DataSet.h"              // for DataSet, Row
#include "common/datatypes/Value.h"                // for Value
#include "graph/context/ExecutionContext.h"        // for ExecutionContext
#include "graph/context/QueryContext.h"            // for QueryContext
#include "graph/context/Result.h"                  // for Result, Result::State
#include "graph/context/Symbols.h"                 // for SymbolTable
#include "graph/executor/query/FilterExecutor.h"   // for FilterExecutor
#include "graph/executor/query/ProjectExecutor.h"  // for ProjectExecutor
#include "graph/executor/test/QueryTestBase.h"     // for QueryTestBase
#include "graph/planner/plan/Query.h"              // for Filter, Project
#include "graph/util/ExpressionUtils.h"            // for ExpressionUtils
#include "parser/Clauses.h"                        // for WhereClause, Yield...
#include "parser/TraverseSentences.h"              // for YieldSentence

namespace nebula {
namespace graph {
class FilterTest_TestEmpty_Test;
class FilterTest_TestGetNeighbors_src_dst_Test;
class FilterTest_TestNullValue_Test;
class FilterTest_TestSequential_Test;

class FilterTest_TestEmpty_Test;
class FilterTest_TestGetNeighbors_src_dst_Test;
class FilterTest_TestNullValue_Test;
class FilterTest_TestSequential_Test;

class FilterTest : public QueryTestBase {
 public:
  void SetUp() override {
    QueryTestBase::SetUp();
  }
};

#define FILTER_RESULT_CHECK(inputName, outputName, sentence, expected)                             \
  do {                                                                                             \
    qctx_->symTable()->newVariable(outputName);                                                    \
    auto yieldSentence = getYieldSentence(sentence, qctx_.get());                                  \
    auto columns = yieldSentence->columns();                                                       \
    for (auto& col : columns) {                                                                    \
      col->setExpr(ExpressionUtils::rewriteLabelAttr2EdgeProp(col->expr()));                       \
    }                                                                                              \
    auto* whereSentence = yieldSentence->where();                                                  \
    whereSentence->setFilter(ExpressionUtils::rewriteLabelAttr2EdgeProp(whereSentence->filter())); \
    auto* filterNode = Filter::make(qctx_.get(), nullptr, yieldSentence->where()->filter());       \
    filterNode->setInputVar(inputName);                                                            \
    filterNode->setOutputVar(outputName);                                                          \
    auto filterExec = std::make_unique<FilterExecutor>(filterNode, qctx_.get());                   \
    EXPECT_TRUE(filterExec->execute().get().ok());                                                 \
    auto& filterResult = qctx_->ectx()->getResult(filterNode->outputVar());                        \
    EXPECT_EQ(filterResult.state(), Result::State::kSuccess);                                      \
                                                                                                   \
    filterNode->setInputVar(outputName);                                                           \
    auto* project = Project::make(qctx_.get(), nullptr, yieldSentence->yieldColumns());            \
    project->setInputVar(filterNode->outputVar());                                                 \
    project->setColNames(std::vector<std::string>{"name"});                                        \
                                                                                                   \
    auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());                         \
    EXPECT_TRUE(proExe->execute().get().ok());                                                     \
    auto& proResult = qctx_->ectx()->getResult(project->outputVar());                              \
                                                                                                   \
    EXPECT_EQ(proResult.value().getDataSet(), expected);                                           \
    EXPECT_EQ(proResult.state(), Result::State::kSuccess);                                         \
  } while (false)

TEST_F(FilterTest, TestGetNeighbors_src_dst) {
  DataSet expected({"name"});
  expected.emplace_back(Row({Value("Ann")}));
  expected.emplace_back(Row({Value("Ann")}));
  expected.emplace_back(Row({Value("Tom")}));
  FILTER_RESULT_CHECK("input_neighbor",
                      "filter_getNeighbor",
                      "YIELD $^.person.name AS name WHERE study.start_year >= 2010",
                      expected);
}

TEST_F(FilterTest, TestSequential) {
  DataSet expected({"name"});
  expected.emplace_back(Row({Value("Ann")}));
  expected.emplace_back(Row({Value("Ann")}));
  FILTER_RESULT_CHECK("input_sequential",
                      "filter_sequential",
                      "YIELD $-.v_name AS name WHERE $-.e_start_year >= 2010",
                      expected);
}

TEST_F(FilterTest, TestNullValue) {
  DataSet expected({"name"});
  FILTER_RESULT_CHECK(
      "input_sequential", "filter_sequential", "YIELD $-.v_name AS name WHERE NULL", expected);
}

TEST_F(FilterTest, TestEmpty) {
  DataSet expected({"name"});
  FILTER_RESULT_CHECK("empty",
                      "filter_empty",
                      "YIELD $^.person.name AS name WHERE study.start_year >= 2010",
                      expected);
}
}  // namespace graph
}  // namespace nebula
