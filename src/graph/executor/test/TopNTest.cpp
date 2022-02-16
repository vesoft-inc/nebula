/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <folly/Try.h>              // for Try::throwUnlessValue
#include <folly/futures/Future.h>   // for Future::get, Future::...
#include <folly/futures/Future.h>   // for Future
#include <folly/futures/Future.h>   // for Future::get, Future::...
#include <folly/futures/Future.h>   // for Future
#include <folly/futures/Promise.h>  // for PromiseException::Pro...
#include <gtest/gtest.h>            // for TestPartResult
#include <gtest/gtest.h>            // for Message
#include <gtest/gtest.h>            // for TestPartResult
#include <stddef.h>                 // for size_t

#include <memory>   // for unique_ptr, allocator
#include <string>   // for string, basic_string
#include <utility>  // for pair, make_pair, move
#include <vector>   // for vector

#include "common/base/Status.h"                 // for Status
#include "common/datatypes/DataSet.h"           // for Row, DataSet
#include "common/datatypes/Value.h"             // for Value
#include "graph/context/ExecutionContext.h"     // for ExecutionContext
#include "graph/context/QueryContext.h"         // for QueryContext
#include "graph/context/Result.h"               // for Result, Result::State
#include "graph/context/Symbols.h"              // for SymbolTable
#include "graph/executor/Executor.h"            // for Executor
#include "graph/executor/test/QueryTestBase.h"  // for QueryTestBase
#include "graph/planner/plan/Logic.h"           // for StartNode
#include "graph/planner/plan/Query.h"           // for TopN, Project
#include "parser/TraverseSentences.h"           // for OrderFactor::OrderType

namespace nebula {
namespace graph {
class TopNTest_topnOneColAsc_Test;
class TopNTest_topnOneColDes_Test;
class TopNTest_topnTwoColDesAsc_Test;
class TopNTest_topnTwoColDesDes_Test;
class TopNTest_topnTwoColsAscAsc_Test;
class TopNTest_topnTwoColsAscDes_Test;

class TopNTest_topnOneColAsc_Test;
class TopNTest_topnOneColDes_Test;
class TopNTest_topnTwoColDesAsc_Test;
class TopNTest_topnTwoColDesDes_Test;
class TopNTest_topnTwoColsAscAsc_Test;
class TopNTest_topnTwoColsAscDes_Test;

class TopNTest : public QueryTestBase {};

#define TOPN_RESULT_CHECK(input_name, outputName, multi, factors, offset, count, expected) \
  do {                                                                                     \
    qctx_->symTable()->newVariable(outputName);                                            \
    auto start = StartNode::make(qctx_.get());                                             \
    auto* topnNode = TopN::make(qctx_.get(), start, factors, offset, count);               \
    topnNode->setInputVar(input_name);                                                     \
    topnNode->setOutputVar(outputName);                                                    \
    auto topnExec = Executor::create(topnNode, qctx_.get());                               \
    EXPECT_TRUE(topnExec->execute().get().ok());                                           \
    auto& topnResult = qctx_->ectx()->getResult(topnNode->outputVar());                    \
    EXPECT_EQ(topnResult.state(), Result::State::kSuccess);                                \
    std::string sentence;                                                                  \
    std::vector<std::string> colNames;                                                     \
    if (multi) {                                                                           \
      sentence = "YIELD $-.v_age AS age, $-.e_start_year AS start_year";                   \
      colNames.emplace_back("age");                                                        \
      colNames.emplace_back("start_year");                                                 \
    } else {                                                                               \
      sentence = "YIELD $-.v_age AS age";                                                  \
      colNames.emplace_back("age");                                                        \
    }                                                                                      \
    auto yieldSentence = getYieldSentence(sentence, qctx_.get());                          \
    auto* project = Project::make(qctx_.get(), start, yieldSentence->yieldColumns());      \
    project->setInputVar(topnNode->outputVar());                                           \
    project->setColNames(std::move(colNames));                                             \
    auto proExe = Executor::create(project, qctx_.get());                                  \
    EXPECT_TRUE(proExe->execute().get().ok());                                             \
    auto& proResult = qctx_->ectx()->getResult(project->outputVar());                      \
    EXPECT_EQ(proResult.value().getDataSet(), expected);                                   \
    EXPECT_EQ(proResult.state(), Result::State::kSuccess);                                 \
  } while (false)

TEST_F(TopNTest, topnOneColAsc) {
  DataSet expected({"age"});
  expected.emplace_back(Row({18}));
  expected.emplace_back(Row({18}));
  expected.emplace_back(Row({19}));
  expected.emplace_back(Row({20}));
  std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
  factors.emplace_back(std::make_pair(2, OrderFactor::OrderType::ASCEND));
  TOPN_RESULT_CHECK("input_sequential", "topn_one_col_asc", false, factors, 0, 4, expected);
}

TEST_F(TopNTest, topnOneColDes) {
  DataSet expected({"age"});
  expected.emplace_back(Row({20}));
  expected.emplace_back(Row({19}));
  expected.emplace_back(Row({18}));
  expected.emplace_back(Row({18}));
  std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
  factors.emplace_back(std::make_pair(2, OrderFactor::OrderType::DESCEND));
  TOPN_RESULT_CHECK("input_sequential", "topn_one_col_des", false, factors, 2, 9, expected);
}

TEST_F(TopNTest, topnTwoColsAscAsc) {
  DataSet expected({"age", "start_year"});
  expected.emplace_back(Row({19, 2009}));
  expected.emplace_back(Row({20, 2008}));
  expected.emplace_back(Row({20, 2009}));
  std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
  factors.emplace_back(std::make_pair(2, OrderFactor::OrderType::ASCEND));
  factors.emplace_back(std::make_pair(4, OrderFactor::OrderType::ASCEND));
  TOPN_RESULT_CHECK("input_sequential", "topn_two_cols_asc_asc", true, factors, 2, 3, expected);
}

TEST_F(TopNTest, topnTwoColsAscDes) {
  DataSet expected({"age", "start_year"});
  expected.emplace_back(Row({18, 2010}));
  expected.emplace_back(Row({18, 2010}));
  std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
  factors.emplace_back(std::make_pair(2, OrderFactor::OrderType::ASCEND));
  factors.emplace_back(std::make_pair(4, OrderFactor::OrderType::DESCEND));
  TOPN_RESULT_CHECK("input_sequential", "topn_two_cols_asc_des", true, factors, 0, 2, expected);
}

TEST_F(TopNTest, topnTwoColDesDes) {
  DataSet expected({"age", "start_year"});
  std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
  factors.emplace_back(std::make_pair(2, OrderFactor::OrderType::DESCEND));
  factors.emplace_back(std::make_pair(4, OrderFactor::OrderType::DESCEND));
  TOPN_RESULT_CHECK("input_sequential", "topn_two_cols_des_des", true, factors, 10, 5, expected);
}

TEST_F(TopNTest, topnTwoColDesAsc) {
  DataSet expected({"age", "start_year"});
  expected.emplace_back(Row({20, 2008}));
  expected.emplace_back(Row({20, 2009}));
  expected.emplace_back(Row({19, 2009}));
  expected.emplace_back(Row({18, 2010}));
  expected.emplace_back(Row({18, 2010}));
  std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
  factors.emplace_back(std::make_pair(2, OrderFactor::OrderType::DESCEND));
  factors.emplace_back(std::make_pair(4, OrderFactor::OrderType::ASCEND));
  TOPN_RESULT_CHECK("input_sequential", "topn_two_cols_des_asc", true, factors, 1, 9, expected);
}
}  // namespace graph
}  // namespace nebula
