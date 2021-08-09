/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/query/LimitExecutor.h"
#include "executor/query/ProjectExecutor.h"
#include "executor/test/QueryTestBase.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {
class LimitTest : public QueryTestBase {
};

#define LIMIT_RESUTL_CHECK(outputName, offset, count, expected)                                    \
    do {                                                                                           \
        qctx_->symTable()->newVariable(outputName);                                                \
        auto start = StartNode::make(qctx_.get());                                                 \
        auto* limitNode = Limit::make(qctx_.get(), start, offset, count);                          \
        limitNode->setInputVar("input_sequential");                                                \
        limitNode->setOutputVar(outputName);                                                       \
        auto limitExec = Executor::create(limitNode, qctx_.get());                                 \
        EXPECT_TRUE(limitExec->execute().get().ok());                                              \
        auto& limitResult = qctx_->ectx()->getResult(limitNode->outputVar());                      \
        EXPECT_EQ(limitResult.state(), Result::State::kSuccess);                                   \
        auto yieldSentence =                                                                       \
            getYieldSentence("YIELD $-.v_name AS name, $-.e_start_year AS start", qctx_.get());    \
        auto columns = yieldSentence->columns();                                                   \
        auto* project = Project::make(qctx_.get(), limitNode, yieldSentence->yieldColumns());      \
        project->setInputVar(limitNode->outputVar());                                              \
        project->setColNames(std::vector<std::string>{"name", "start"});                           \
        auto proExe = Executor::create(project, qctx_.get());                                      \
        EXPECT_TRUE(proExe->execute().get().ok());                                                 \
        auto& proResult = qctx_->ectx()->getResult(project->outputVar());                          \
        EXPECT_EQ(proResult.value().getDataSet(), expected);                                       \
        EXPECT_EQ(proResult.state(), Result::State::kSuccess);                                     \
    } while (false)

TEST_F(LimitTest, SequentialInRange1) {
    DataSet expected({"name", "start"});
    expected.emplace_back(Row({Value("Joy"), Value(2009)}));
    expected.emplace_back(Row({Value("Tom"), Value(2008)}));
    LIMIT_RESUTL_CHECK("limit_in_sequential1", 1, 2, expected);
}

TEST_F(LimitTest, SequentialInRange2) {
    DataSet expected({"name", "start"});
    expected.emplace_back(Row({Value("Ann"), Value(2010)}));
    expected.emplace_back(Row({Value("Joy"), Value(2009)}));
    expected.emplace_back(Row({Value("Tom"), Value(2008)}));
    expected.emplace_back(Row({Value("Kate"), Value(2009)}));
    LIMIT_RESUTL_CHECK("limit_in_sequential2", 0, 4, expected);
}

TEST_F(LimitTest, SequentialOutRange1) {
    DataSet expected({"name", "start"});
    expected.emplace_back(Row({Value("Ann"), Value(2010)}));
    expected.emplace_back(Row({Value("Joy"), Value(2009)}));
    expected.emplace_back(Row({Value("Tom"), Value(2008)}));
    expected.emplace_back(Row({Value("Kate"), Value(2009)}));
    expected.emplace_back(Row({Value("Ann"), Value(2010)}));
    expected.emplace_back(Row({Value("Lily"), Value(2009)}));
    LIMIT_RESUTL_CHECK("limit_out_sequential1", 0, 7, expected);
}

TEST_F(LimitTest, getNeighborOutRange2) {
    DataSet expected({"name", "start"});
    LIMIT_RESUTL_CHECK("limit_out_sequential2", 6, 2, expected);
}
}  // namespace graph
}  // namespace nebula
