/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/query/ProjectExecutor.h"
#include "executor/query/TopNExecutor.h"
#include "executor/test/QueryTestBase.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

class TopNTest : public QueryTestBase {};

#define TOPN_RESUTL_CHECK(input_name, outputName, multi, factors, offset, count, expected)         \
    do {                                                                                           \
        qctx_->symTable()->newVariable(outputName);                                                \
        auto start = StartNode::make(qctx_.get());                                                 \
        auto* topnNode = TopN::make(qctx_.get(), start, factors, offset, count);                   \
        topnNode->setInputVar(input_name);                                                         \
        topnNode->setOutputVar(outputName);                                                        \
        auto topnExec = Executor::create(topnNode, qctx_.get());                                   \
        EXPECT_TRUE(topnExec->execute().get().ok());                                               \
        auto& topnResult = qctx_->ectx()->getResult(topnNode->outputVar());                        \
        EXPECT_EQ(topnResult.state(), Result::State::kSuccess);                                    \
        std::string sentence;                                                                      \
        std::vector<std::string> colNames;                                                         \
        if (multi) {                                                                               \
            sentence = "YIELD $-.v_age AS age, $-.e_start_year AS start_year";                     \
            colNames.emplace_back("age");                                                          \
            colNames.emplace_back("start_year");                                                   \
        } else {                                                                                   \
            sentence = "YIELD $-.v_age AS age";                                                    \
            colNames.emplace_back("age");                                                          \
        }                                                                                          \
        auto yieldSentence = getYieldSentence(sentence, qctx_.get());                              \
        auto* project = Project::make(qctx_.get(), start, yieldSentence->yieldColumns());          \
        project->setInputVar(topnNode->outputVar());                                               \
        project->setColNames(std::move(colNames));                                                 \
        auto proExe = Executor::create(project, qctx_.get());                                      \
        EXPECT_TRUE(proExe->execute().get().ok());                                                 \
        auto& proResult = qctx_->ectx()->getResult(project->outputVar());                          \
        EXPECT_EQ(proResult.value().getDataSet(), expected);                                       \
        EXPECT_EQ(proResult.state(), Result::State::kSuccess);                                     \
    } while (false)

TEST_F(TopNTest, topnOneColAsc) {
    DataSet expected({"age"});
    expected.emplace_back(Row({18}));
    expected.emplace_back(Row({18}));
    expected.emplace_back(Row({19}));
    expected.emplace_back(Row({20}));
    std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
    factors.emplace_back(std::make_pair(2, OrderFactor::OrderType::ASCEND));
    TOPN_RESUTL_CHECK("input_sequential", "topn_one_col_asc", false, factors, 0, 4, expected);
}

TEST_F(TopNTest, topnOneColDes) {
    DataSet expected({"age"});
    expected.emplace_back(Row({20}));
    expected.emplace_back(Row({19}));
    expected.emplace_back(Row({18}));
    expected.emplace_back(Row({18}));
    std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
    factors.emplace_back(std::make_pair(2, OrderFactor::OrderType::DESCEND));
    TOPN_RESUTL_CHECK("input_sequential", "topn_one_col_des", false, factors, 2, 9, expected);
}

TEST_F(TopNTest, topnTwoColsAscAsc) {
    DataSet expected({"age", "start_year"});
    expected.emplace_back(Row({19, 2009}));
    expected.emplace_back(Row({20, 2008}));
    expected.emplace_back(Row({20, 2009}));
    std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
    factors.emplace_back(std::make_pair(2, OrderFactor::OrderType::ASCEND));
    factors.emplace_back(std::make_pair(4, OrderFactor::OrderType::ASCEND));
    TOPN_RESUTL_CHECK("input_sequential", "topn_two_cols_asc_asc", true, factors, 2, 3, expected);
}

TEST_F(TopNTest, topnTwoColsAscDes) {
    DataSet expected({"age", "start_year"});
    expected.emplace_back(Row({18, 2010}));
    expected.emplace_back(Row({18, 2010}));
    std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
    factors.emplace_back(std::make_pair(2, OrderFactor::OrderType::ASCEND));
    factors.emplace_back(std::make_pair(4, OrderFactor::OrderType::DESCEND));
    TOPN_RESUTL_CHECK("input_sequential", "topn_two_cols_asc_des", true, factors, 0, 2, expected);
}

TEST_F(TopNTest, topnTwoColDesDes) {
    DataSet expected({"age", "start_year"});
    std::vector<std::pair<size_t, OrderFactor::OrderType>> factors;
    factors.emplace_back(std::make_pair(2, OrderFactor::OrderType::DESCEND));
    factors.emplace_back(std::make_pair(4, OrderFactor::OrderType::DESCEND));
    TOPN_RESUTL_CHECK("input_sequential", "topn_two_cols_des_des", true, factors, 10, 5, expected);
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
    TOPN_RESUTL_CHECK("input_sequential", "topn_two_cols_des_asc", true, factors, 1, 9, expected);
}
}   // namespace graph
}   // namespace nebula
