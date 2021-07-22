/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/query/FilterExecutor.h"
#include "executor/query/ProjectExecutor.h"
#include "executor/test/QueryTestBase.h"
#include "planner/plan/Query.h"
#include "util/ExpressionUtils.h"

namespace nebula {
namespace graph {

class FilterTest : public QueryTestBase {
public:
    void SetUp() override {
        QueryTestBase::SetUp();
    }
};

#define FILTER_RESUTL_CHECK(inputName, outputName, sentence, expected)                             \
    do {                                                                                           \
        qctx_->symTable()->newVariable(outputName);                                                \
        auto yieldSentence = getYieldSentence(sentence, qctx_.get());                              \
        auto columns = yieldSentence->columns();                                                   \
        for (auto& col : columns) {                                                                \
            col->setExpr(ExpressionUtils::rewriteLabelAttr2EdgeProp(col->expr()));                 \
        }                                                                                          \
        auto* whereSentence = yieldSentence->where();                                              \
        whereSentence->setFilter(                                                                  \
            ExpressionUtils::rewriteLabelAttr2EdgeProp(whereSentence->filter()));                  \
        auto* filterNode = Filter::make(qctx_.get(), nullptr, yieldSentence->where()->filter());   \
        filterNode->setInputVar(inputName);                                                        \
        filterNode->setOutputVar(outputName);                                                      \
        auto filterExec = std::make_unique<FilterExecutor>(filterNode, qctx_.get());               \
        EXPECT_TRUE(filterExec->execute().get().ok());                                             \
        auto& filterResult = qctx_->ectx()->getResult(filterNode->outputVar());                    \
        EXPECT_EQ(filterResult.state(), Result::State::kSuccess);                                  \
                                                                                                   \
        filterNode->setInputVar(outputName);                                                       \
        auto* project = Project::make(qctx_.get(), nullptr, yieldSentence->yieldColumns());        \
        project->setInputVar(filterNode->outputVar());                                             \
        project->setColNames(std::vector<std::string>{"name"});                                    \
                                                                                                   \
        auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());                     \
        EXPECT_TRUE(proExe->execute().get().ok());                                                 \
        auto& proSesult = qctx_->ectx()->getResult(project->outputVar());                          \
                                                                                                   \
        EXPECT_EQ(proSesult.value().getDataSet(), expected);                                       \
        EXPECT_EQ(proSesult.state(), Result::State::kSuccess);                                     \
    } while (false)

TEST_F(FilterTest, TestGetNeighbors_src_dst) {
    DataSet expected({"name"});
    expected.emplace_back(Row({Value("Ann")}));
    expected.emplace_back(Row({Value("Ann")}));
    expected.emplace_back(Row({Value("Tom")}));
    FILTER_RESUTL_CHECK("input_neighbor",
                        "filter_getNeighbor",
                        "YIELD $^.person.name AS name WHERE study.start_year >= 2010",
                        expected);
}

TEST_F(FilterTest, TestSequential) {
    DataSet expected({"name"});
    expected.emplace_back(Row({Value("Ann")}));
    expected.emplace_back(Row({Value("Ann")}));
    FILTER_RESUTL_CHECK("input_sequential",
                        "filter_sequential",
                        "YIELD $-.v_name AS name WHERE $-.e_start_year >= 2010",
                        expected);
}

TEST_F(FilterTest, TestNullValue) {
    DataSet expected({"name"});
    FILTER_RESUTL_CHECK(
        "input_sequential", "filter_sequential", "YIELD $-.v_name AS name WHERE NULL", expected);
}

TEST_F(FilterTest, TestEmpty) {
    DataSet expected({"name"});
    FILTER_RESUTL_CHECK("empty",
                        "filter_empty",
                        "YIELD $^.person.name AS name WHERE study.start_year >= 2010",
                        expected);
}
}   // namespace graph
}   // namespace nebula
