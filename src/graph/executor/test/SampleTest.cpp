/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>

#include "graph/context/QueryContext.h"
#include "graph/executor/query/ProjectExecutor.h"
#include "graph/executor/query/SampleExecutor.h"
#include "graph/executor/test/QueryTestBase.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/Query.h"
#include "graph/util/ExpressionUtils.h"

namespace nebula {
namespace graph {
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
