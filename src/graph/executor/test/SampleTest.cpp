/* Copyright (c) 2021 vesoft inc. All rights reserved.
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

#include <memory>  // for unique_ptr, allocator
#include <string>  // for string, basic_string
#include <vector>  // for vector

#include "common/base/Status.h"                 // for Status
#include "common/datatypes/DataSet.h"           // for DataSet
#include "common/datatypes/Value.h"             // for Value
#include "graph/context/ExecutionContext.h"     // for ExecutionContext
#include "graph/context/QueryContext.h"         // for QueryContext
#include "graph/context/Result.h"               // for Result, Result::State
#include "graph/context/Symbols.h"              // for SymbolTable
#include "graph/executor/Executor.h"            // for Executor
#include "graph/executor/test/QueryTestBase.h"  // for QueryTestBase
#include "graph/planner/plan/Logic.h"           // for StartNode
#include "graph/planner/plan/Query.h"           // for Sample, Project
#include "parser/TraverseSentences.h"           // for YieldSentence

namespace nebula {
namespace graph {
class SampleTest_GN2_Test;
class SampleTest_GNOutOfRange1_Test;
class SampleTest_GNOutOfRange2_Test;
class SampleTest_SequentialInRange2_Test;
class SampleTest_SequentialOutRange1_Test;
class SampleTest_SequentialOutRange2_Test;

class SampleTest_GN2_Test;
class SampleTest_GNOutOfRange1_Test;
class SampleTest_GNOutOfRange2_Test;
class SampleTest_SequentialInRange2_Test;
class SampleTest_SequentialOutRange1_Test;
class SampleTest_SequentialOutRange2_Test;

class SampleTest : public QueryTestBase {};

#define SAMPLE_RESULT_CHECK(outputName, count, expect)                                      \
  do {                                                                                      \
    qctx_->symTable()->newVariable(outputName);                                             \
    auto start = StartNode::make(qctx_.get());                                              \
    auto* sampleNode = Sample::make(qctx_.get(), start, count);                             \
    sampleNode->setInputVar("input_sequential");                                            \
    sampleNode->setOutputVar(outputName);                                                   \
    auto sampleExec = Executor::create(sampleNode, qctx_.get());                            \
    EXPECT_TRUE(sampleExec->execute().get().ok());                                          \
    auto& sampleResult = qctx_->ectx()->getResult(sampleNode->outputVar());                 \
    EXPECT_EQ(sampleResult.state(), Result::State::kSuccess);                               \
    auto yieldSentence =                                                                    \
        getYieldSentence("YIELD $-.v_name AS name, $-.e_start_year AS start", qctx_.get()); \
    auto columns = yieldSentence->columns();                                                \
    auto* project = Project::make(qctx_.get(), sampleNode, yieldSentence->yieldColumns());  \
    project->setInputVar(sampleNode->outputVar());                                          \
    project->setColNames(std::vector<std::string>{"name", "start"});                        \
    auto proExe = Executor::create(project, qctx_.get());                                   \
    EXPECT_TRUE(proExe->execute().get().ok());                                              \
    auto& proResult = qctx_->ectx()->getResult(project->outputVar());                       \
    EXPECT_EQ(proResult.value().getDataSet().size(), expect);                               \
    EXPECT_EQ(proResult.state(), Result::State::kSuccess);                                  \
  } while (false)

TEST_F(SampleTest, SequentialInRange2) {
  SAMPLE_RESULT_CHECK("sample_in_sequential2", 4, 4);
}

TEST_F(SampleTest, SequentialOutRange1) {
  SAMPLE_RESULT_CHECK("sample_out_sequential3", 7, 6);
}

TEST_F(SampleTest, SequentialOutRange2) {
  SAMPLE_RESULT_CHECK("sample_out_sequential4", 8, 6);
}

#define SAMPLE_GN_RESULT_CHECK(outputName, count, expect)                                  \
  do {                                                                                     \
    qctx_->symTable()->newVariable(outputName);                                            \
    auto start = StartNode::make(qctx_.get());                                             \
    auto* sampleNode = Sample::make(qctx_.get(), start, count);                            \
    sampleNode->setInputVar("input_neighbor");                                             \
    sampleNode->setOutputVar(outputName);                                                  \
    auto sampleExec = Executor::create(sampleNode, qctx_.get());                           \
    EXPECT_TRUE(sampleExec->execute().get().ok());                                         \
    auto& sampleResult = qctx_->ectx()->getResult(sampleNode->outputVar());                \
    EXPECT_EQ(sampleResult.state(), Result::State::kSuccess);                              \
    auto yieldSentence = getYieldSentence("YIELD study._dst AS dst", qctx_.get());         \
    auto columns = yieldSentence->columns();                                               \
    auto* project = Project::make(qctx_.get(), sampleNode, yieldSentence->yieldColumns()); \
    project->setInputVar(sampleNode->outputVar());                                         \
    project->setColNames(std::vector<std::string>{"dst"});                                 \
    auto proExe = Executor::create(project, qctx_.get());                                  \
    EXPECT_TRUE(proExe->execute().get().ok());                                             \
    auto& proResult = qctx_->ectx()->getResult(project->outputVar());                      \
    EXPECT_EQ(proResult.value().getDataSet().size(), expect);                              \
    EXPECT_EQ(proResult.state(), Result::State::kSuccess);                                 \
  } while (false)

TEST_F(SampleTest, GN2) {
  SAMPLE_GN_RESULT_CHECK("sample_in_gn2", 4, 4);
}

TEST_F(SampleTest, GNOutOfRange1) {
  SAMPLE_GN_RESULT_CHECK("sample_out_gn1", 6, 4);
}

TEST_F(SampleTest, GNOutOfRange2) {
  SAMPLE_GN_RESULT_CHECK("sample_out_gn2", 7, 4);
}

}  // namespace graph
}  // namespace nebula
