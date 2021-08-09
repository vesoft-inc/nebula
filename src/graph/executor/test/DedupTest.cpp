/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/plan/Query.h"
#include "executor/query/DedupExecutor.h"
#include "executor/test/QueryTestBase.h"
#include "executor/query/ProjectExecutor.h"

namespace nebula {
namespace graph {

class DedupTest : public QueryTestBase {
public:
    void SetUp() override {
        QueryTestBase::SetUp();
    }
};

#define DEDUP_RESUTL_CHECK(inputName, outputName, sentence, expected)                              \
    do {                                                                                           \
        qctx_->symTable()->newVariable(outputName);                                                \
        auto yieldSentence = getYieldSentence(sentence, qctx_.get());                              \
        auto* dedupNode = Dedup::make(qctx_.get(), nullptr);                                       \
        dedupNode->setInputVar(inputName);                                                         \
        dedupNode->setOutputVar(outputName);                                                       \
        auto dedupExec = std::make_unique<DedupExecutor>(dedupNode, qctx_.get());                  \
        if (!expected.colNames.empty()) {                                                          \
            EXPECT_TRUE(dedupExec->execute().get().ok());                                          \
        } else {                                                                                   \
            EXPECT_FALSE(dedupExec->execute().get().ok());                                         \
            return;                                                                                \
        }                                                                                          \
        auto& dedupResult = qctx_->ectx()->getResult(dedupNode->outputVar());                      \
        EXPECT_EQ(dedupResult.state(), Result::State::kSuccess);                                   \
                                                                                                   \
        dedupNode->setInputVar(outputName);                                                        \
        auto* project = Project::make(qctx_.get(), nullptr, yieldSentence->yieldColumns());        \
        project->setInputVar(dedupNode->outputVar());                                              \
        auto colNames = expected.colNames;                                                         \
        project->setColNames(std::move(colNames));                                                 \
                                                                                                   \
        auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());                     \
        EXPECT_TRUE(proExe->execute().get().ok());                                                 \
        auto& proSesult = qctx_->ectx()->getResult(project->outputVar());                          \
                                                                                                   \
        EXPECT_EQ(proSesult.value().getDataSet(), expected);                                       \
        EXPECT_EQ(proSesult.state(), Result::State::kSuccess);                                     \
    } while (false)

TEST_F(DedupTest, TestSequential) {
    DataSet expected({"vid", "name", "age", "dst", "start", "end"});
    expected.emplace_back(Row({"Ann", "Ann", 18, "School1", 2010, 2014}));
    expected.emplace_back(Row({"Joy", "Joy", Value::kNullValue, "School2", 2009, 2012}));
    expected.emplace_back(Row({"Tom", "Tom", 20, "School2", 2008, 2012}));
    expected.emplace_back(Row({"Kate", "Kate", 19, "School2", 2009, 2013}));
    expected.emplace_back(Row({"Lily", "Lily", 20, "School2", 2009, 2012}));

    auto sentence = "YIELD DISTINCT $-.vid as vid, $-.v_name as name, $-.v_age as age, "
                    "$-.v_dst as dst, $-.e_start_year as start, $-.e_end_year as end";
    DEDUP_RESUTL_CHECK("input_sequential",
                       "dedup_sequential",
                       sentence,
                       expected);
}

TEST_F(DedupTest, TestEmpty) {
    DataSet expected({"name"});
    DEDUP_RESUTL_CHECK("empty",
                       "dedup_sequential",
                       "YIELD DISTINCT $-.v_dst as name",
                       expected);
}

TEST_F(DedupTest, WrongTypeIterator) {
    DataSet expected;
    DEDUP_RESUTL_CHECK("input_neighbor",
                       "dedup_sequential",
                       "YIELD DISTINCT $-.v_dst as name",
                       expected);
}
}  // namespace graph
}  // namespace nebula
