/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Query.h"
#include "exec/query/SortExecutor.h"
#include "exec/query/ProjectExecutor.h"
#include "exec/query/test/QueryTestBase.h"

namespace nebula {
namespace graph {
class SortTest : public QueryTestBase {
};

#define SORT_RESUTL_CHECK(input_name, outputName, multi, factors, expected)           \
    do {                                                                              \
        auto* plan = qctx_->plan();                                                   \
        auto* sortNode = Sort::make(plan, nullptr, factors);                          \
        sortNode->setInputVar(input_name);                                            \
        sortNode->setOutputVar(outputName);                                           \
        auto sortExec = std::make_unique<SortExecutor>(sortNode, qctx_.get());        \
        EXPECT_TRUE(sortExec->execute().get().ok());                                  \
        auto& sortResult = qctx_->ectx()->getResult(sortNode->varName());             \
        EXPECT_EQ(sortResult.state(), Result::State::kSuccess);                       \
        std::string sentence;                                                         \
        std::vector<std::string> colNames;                                            \
        if (multi) {                                                                  \
            sentence = "YIELD $-.v_age AS age, $-.e_start_year AS start_year";        \
            colNames.emplace_back("age");                                             \
            colNames.emplace_back("start_year");                                      \
        } else {                                                                      \
            sentence = "YIELD $-.v_age AS age";                                       \
            colNames.emplace_back("age");                                             \
        }                                                                             \
        auto yieldSentence = getYieldSentence(sentence);                              \
        auto* project = Project::make(plan, nullptr, yieldSentence->yieldColumns());  \
        project->setInputVar(sortNode->varName());                                    \
        project->setColNames(std::move(colNames));                                    \
        auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());        \
        EXPECT_TRUE(proExe->execute().get().ok());                                    \
        auto& proResult = qctx_->ectx()->getResult(project->varName());               \
        EXPECT_EQ(proResult.value().getDataSet(), expected);                          \
        EXPECT_EQ(proResult.state(), Result::State::kSuccess);                        \
    } while (false)


TEST_F(SortTest, sortOneColAsc) {
    DataSet expected({"age"});
    expected.emplace_back(Row({18}));
    expected.emplace_back(Row({18}));
    expected.emplace_back(Row({19}));
    expected.emplace_back(Row({20}));
    expected.emplace_back(Row({20}));
    expected.emplace_back(Row({Value::kNullValue}));
    std::vector<std::pair<std::string, OrderFactor::OrderType>> factors;
    factors.emplace_back(std::make_pair("v_age", OrderFactor::OrderType::ASCEND));
    SORT_RESUTL_CHECK("input_sequential", "sort_one_col_asc", false, factors, expected);
}

TEST_F(SortTest, sortOneColDes) {
    DataSet expected({"age"});
    expected.emplace_back(Row({Value::kNullValue}));
    expected.emplace_back(Row({20}));
    expected.emplace_back(Row({20}));
    expected.emplace_back(Row({19}));
    expected.emplace_back(Row({18}));
    expected.emplace_back(Row({18}));
    std::vector<std::pair<std::string, OrderFactor::OrderType>> factors;
    factors.emplace_back(std::make_pair("v_age", OrderFactor::OrderType::DESCEND));
    SORT_RESUTL_CHECK("input_sequential", "sort_one_col_des", false, factors, expected);
}

TEST_F(SortTest, sortTwoColsAscAsc) {
    DataSet expected({"age", "start_year"});
    expected.emplace_back(Row({18, 2010}));
    expected.emplace_back(Row({18, 2010}));
    expected.emplace_back(Row({19, 2009}));
    expected.emplace_back(Row({20, 2008}));
    expected.emplace_back(Row({20, 2009}));
    expected.emplace_back(Row({Value::kNullValue, 2009}));
    std::vector<std::pair<std::string, OrderFactor::OrderType>> factors;
    factors.emplace_back(std::make_pair("v_age", OrderFactor::OrderType::ASCEND));
    factors.emplace_back(std::make_pair("e_start_year", OrderFactor::OrderType::ASCEND));
    SORT_RESUTL_CHECK("input_sequential", "sort_two_cols_asc_asc", true, factors, expected);
}

TEST_F(SortTest, sortTwoColsAscDes) {
    DataSet expected({"age", "start_year"});
    expected.emplace_back(Row({18, 2010}));
    expected.emplace_back(Row({18, 2010}));
    expected.emplace_back(Row({19, 2009}));
    expected.emplace_back(Row({20, 2009}));
    expected.emplace_back(Row({20, 2008}));
    expected.emplace_back(Row({Value::kNullValue, 2009}));
    std::vector<std::pair<std::string, OrderFactor::OrderType>> factors;
    factors.emplace_back(std::make_pair("v_age", OrderFactor::OrderType::ASCEND));
    factors.emplace_back(std::make_pair("e_start_year", OrderFactor::OrderType::DESCEND));
    SORT_RESUTL_CHECK("input_sequential", "sort_two_cols_asc_des", true, factors, expected);
}

TEST_F(SortTest, sortTwoColDesDes) {
    DataSet expected({"age", "start_year"});
    expected.emplace_back(Row({Value::kNullValue, 2009}));
    expected.emplace_back(Row({20, 2009}));
    expected.emplace_back(Row({20, 2008}));
    expected.emplace_back(Row({19, 2009}));
    expected.emplace_back(Row({18, 2010}));
    expected.emplace_back(Row({18, 2010}));
    std::vector<std::pair<std::string, OrderFactor::OrderType>> factors;
    factors.emplace_back(std::make_pair("v_age", OrderFactor::OrderType::DESCEND));
    factors.emplace_back(std::make_pair("e_start_year", OrderFactor::OrderType::DESCEND));
    SORT_RESUTL_CHECK("input_sequential", "sort_two_cols_des_des", true, factors, expected);
}

TEST_F(SortTest, sortTwoColDesDes_union) {
    DataSet expected({"age", "start_year"});
    expected.emplace_back(Row({Value::kNullValue, 2009}));
    expected.emplace_back(Row({20, 2009}));
    expected.emplace_back(Row({20, 2008}));
    expected.emplace_back(Row({19, 2009}));
    expected.emplace_back(Row({18, 2010}));
    expected.emplace_back(Row({18, 2010}));
    std::vector<std::pair<std::string, OrderFactor::OrderType>> factors;
    factors.emplace_back(std::make_pair("v_age", OrderFactor::OrderType::DESCEND));
    factors.emplace_back(std::make_pair("e_start_year", OrderFactor::OrderType::DESCEND));
    SORT_RESUTL_CHECK("union_sequential", "union_sort_two_cols_des_des", true, factors, expected);
}
}  // namespace graph
}  // namespace nebula
