/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/query/ProjectExecutor.h"
#include "executor/test/QueryTestBase.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

class ProjectTest : public QueryTestBase {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        start_ = StartNode::make(qctx_.get());

        {
            DataSet ds;
            ds.colNames = {"vid", "col2"};
            for (auto i = 0; i < 10; ++i) {
                Row row;
                row.values.emplace_back(i);
                row.values.emplace_back(i + 1);
                ds.rows.emplace_back(std::move(row));
            }
            ResultBuilder builder;
            builder.value(Value(std::move(ds)));
            qctx_->symTable()->newVariable("input_project");
            qctx_->ectx()->setResult("input_project", builder.finish());
        }
        {
            DataSet ds;
            ds.colNames = {"vid", "col2"};
            ResultBuilder builder;
            builder.value(Value(std::move(ds)));
            qctx_->symTable()->newVariable("empty");
            qctx_->ectx()->setResult("empty", builder.finish());
        }
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
    StartNode* start_;
};

TEST_F(ProjectTest, Project1Col) {
    std::string input = "input_project";
    auto yieldColumns = getYieldColumns("YIELD $input_project.vid AS vid", qctx_.get());

    auto* project = Project::make(qctx_.get(), start_, yieldColumns);
    project->setInputVar(input);
    project->setColNames(std::vector<std::string>{"vid"});

    auto proExe = Executor::create(project, qctx_.get());
    auto future = proExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(project->outputVar());

    DataSet expected;
    expected.colNames = {"vid"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.values.emplace_back(i);
        expected.rows.emplace_back(std::move(row));
    }
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(ProjectTest, Project2Col) {
    std::string input = "input_project";
    auto yieldColumns =
        getYieldColumns("YIELD $input_project.vid AS vid, $input_project.col2 AS num", qctx_.get());
    auto* project = Project::make(qctx_.get(), start_, yieldColumns);
    project->setInputVar(input);
    project->setColNames(std::vector<std::string>{"vid", "num"});

    auto proExe = Executor::create(project, qctx_.get());
    auto future = proExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(project->outputVar());

    DataSet expected;
    expected.colNames = {"vid", "num"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.values.emplace_back(i);
        row.values.emplace_back(i + 1);
        expected.rows.emplace_back(std::move(row));
    }
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(ProjectTest, EmptyInput) {
    std::string input = "empty";
    auto yieldColumns = getYieldColumns("YIELD $input_project.vid AS vid", qctx_.get());
    auto* project = Project::make(qctx_.get(), start_, std::move(yieldColumns));
    project->setInputVar(input);
    project->setColNames(std::vector<std::string>{"vid"});

    auto proExe = Executor::create(project, qctx_.get());
    auto future = proExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(project->outputVar());

    DataSet expected;
    expected.colNames.emplace_back("vid");
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

}  // namespace graph
}  // namespace nebula
