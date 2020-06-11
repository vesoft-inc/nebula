/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Query.h"
#include "exec/query/ProjectExecutor.h"

namespace nebula {
namespace graph {
class ProjectTest : public testing::Test {
protected:
    static void SetUpTestCase() {
        qctx_ = std::make_unique<QueryContext>();

        {
            DataSet ds;
            ds.colNames = {"_vid", "col2"};
            for (auto i = 0; i < 10; ++i) {
                Row row;
                row.columns.emplace_back(i);
                row.columns.emplace_back(i + 1);
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("input_project",
                        ExecResult::buildSequential(Value(std::move(ds)), State()));
        }
        {
            DataSet ds;
            ds.colNames = {"_vid", "col2"};
            qctx_->ectx()->setResult("empty",
                        ExecResult::buildSequential(Value(std::move(ds)), State()));
        }
    }

protected:
    static std::unique_ptr<QueryContext> qctx_;
};

std::unique_ptr<QueryContext> ProjectTest::qctx_;

TEST_F(ProjectTest, Project1Col) {
    std::string input = "input_project";
    auto* columns = new YieldColumns();
    auto* column = new YieldColumn(
            new VariablePropertyExpression(
                new std::string(input),
                new std::string("_vid")),
            new std::string("_vid"));
    columns->addColumn(column);
    auto* plan = qctx_->plan();
    auto* project = Project::make(plan, nullptr, plan->saveObject(columns));
    project->setInputVar(input);
    project->setColNames(std::vector<std::string>{"_vid"});

    auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());
    auto future = proExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(project->varName());

    DataSet expected;
    expected.colNames = {"_vid"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.columns.emplace_back(i);
        expected.rows.emplace_back(std::move(row));
    }
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
}

TEST_F(ProjectTest, Project2Col) {
    std::string input = "input_project";
    auto* columns = new YieldColumns();
    auto* column0 = new YieldColumn(
            new VariablePropertyExpression(
                new std::string(input),
                new std::string("_vid")),
            new std::string("_vid"));
    columns->addColumn(column0);
    auto* column1 = new YieldColumn(
            new VariablePropertyExpression(
                new std::string(input),
                new std::string("col2")),
            new std::string("num"));
    columns->addColumn(column1);
    auto* plan = qctx_->plan();
    auto* project = Project::make(plan, nullptr, plan->saveObject(columns));
    project->setInputVar(input);
    project->setColNames(std::vector<std::string>{"_vid", "num"});

    auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());
    auto future = proExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(project->varName());

    DataSet expected;
    expected.colNames = {"_vid", "num"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.columns.emplace_back(i);
        row.columns.emplace_back(i + 1);
        expected.rows.emplace_back(std::move(row));
    }
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
}

TEST_F(ProjectTest, EmptyInput) {
    std::string input = "empty";
    auto* columns = new YieldColumns();
    auto* column = new YieldColumn(
            new VariablePropertyExpression(
                new std::string(input),
                new std::string("_vid")),
            new std::string("_vid"));
    columns->addColumn(column);
    auto* plan = qctx_->plan();
    auto* project = Project::make(plan, nullptr, plan->saveObject(columns));
    project->setInputVar(input);
    project->setColNames(std::vector<std::string>{"_vid"});

    auto proExe = std::make_unique<ProjectExecutor>(project, qctx_.get());
    auto future = proExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(project->varName());

    DataSet expected;
    expected.colNames.emplace_back("_vid");
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state().stat(), State::Stat::kSuccess);
}
}  // namespace graph
}  // namespace nebula
