/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/query/UnwindExecutor.h"
#include "executor/test/QueryTestBase.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

class UnwindTest : public QueryTestBase {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        start_ = StartNode::make(qctx_.get());
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
    StartNode* start_;
};

TEST_F(UnwindTest, UnwindList) {
    // UNWIND [1, 2, 3] as r
    auto exprList = new ExpressionList(3);
    for (auto i = 0; i < 3; ++i) {
        exprList->add(new ConstantExpression(i));
    }

    auto *col = new YieldColumn(new ListExpression(exprList), new std::string("r"));
    auto *columns = new YieldColumns();
    qctx_->objPool()->add(columns);
    columns->addColumn(col);

    auto* unwind = Unwind::make(qctx_.get(), start_, columns);
    unwind->setColNames(std::vector<std::string>{"r"});

    auto unwExe = Executor::create(unwind, qctx_.get());
    auto future = unwExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(unwind->outputVar());

    DataSet expected;
    expected.colNames = {"r"};
    for (auto i = 0; i < 3; ++i) {
        Row row;
        row.values.emplace_back(i);
        expected.rows.emplace_back(std::move(row));
    }
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(UnwindTest, UnwindNestedList) {
    // UNWIND [1, [2, NULL, 3], 4, NULL] as r
    auto *nestedList = new ExpressionList(3);
    nestedList->add(new ConstantExpression(2));
    nestedList->add(new ConstantExpression(Value::kNullValue));
    nestedList->add(new ConstantExpression(3));
    auto *nestedExpr = new ListExpression(nestedList);

    auto *exprList = new ExpressionList(4);
    exprList->add(new ConstantExpression(1));
    exprList->add(nestedExpr);
    exprList->add(new ConstantExpression(4));
    exprList->add(new ConstantExpression(Value::kNullValue));
    auto *col = new YieldColumn(new ListExpression(exprList), new std::string("r"));
    auto *columns = new YieldColumns();
    qctx_->objPool()->add(columns);
    columns->addColumn(col);

    auto* unwind = Unwind::make(qctx_.get(), start_, columns);
    unwind->setColNames(std::vector<std::string>{"r"});

    auto unwExe = Executor::create(unwind, qctx_.get());
    auto future = unwExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(unwind->outputVar());

    DataSet expected;
    expected.colNames = {"r"};
    expected.rows = {Row({Value(1)}),
                     Row({Value(List({Value(2), Value::kNullValue, Value(3)}))}),
                     Row({Value(4)}),
                     Row({Value::kNullValue})};
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(UnwindTest, UnwindLabel) {
    // UNWIND [1, [2, NULL, 3], 4, NULL] as r1 UNWIND r as r2
    auto *nestedList = new ExpressionList(3);
    nestedList->add(new ConstantExpression(2));
    nestedList->add(new ConstantExpression(Value::kNullValue));
    nestedList->add(new ConstantExpression(3));
    auto *nestedExpr = new ListExpression(nestedList);

    auto *exprList = new ExpressionList(4);
    exprList->add(new ConstantExpression(1));
    exprList->add(nestedExpr);
    exprList->add(new ConstantExpression(4));
    exprList->add(new ConstantExpression(Value::kNullValue));
    auto *col = new YieldColumn(new ListExpression(exprList), new std::string("r1"));
    auto *columns = new YieldColumns();
    qctx_->objPool()->add(columns);
    columns->addColumn(col);

    auto* unwind1 = Unwind::make(qctx_.get(), start_, columns);
    unwind1->setColNames(std::vector<std::string>{"r1"});
    auto unwExe = Executor::create(unwind1, qctx_.get());
    auto future = unwExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());

    auto col2 = new YieldColumn(
        new VariablePropertyExpression(new std::string(), new std::string("r1")));
    columns = new YieldColumns();
    qctx_->objPool()->add(columns);
    columns->addColumn(col2);
    auto* unwind2 = Unwind::make(qctx_.get(), unwind1, columns);
    unwind2->setInputVar(unwind1->outputVar());
    unwind2->setColNames(std::vector<std::string>{"r2"});
    unwExe = Executor::create(unwind2, qctx_.get());
    future = unwExe->execute();
    status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(unwind2->outputVar());

    DataSet expected;
    expected.colNames = {"r2"};
    expected.rows = {Row({Value(1)}),
                     Row({Value(2)}),
                     Row({Value::kNullValue}),
                     Row({Value(3)}),
                     Row({Value(4)})};
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(UnwindTest, ProjectUnwind) {
    /*
    | a | b      |
    | 1 | [1,2,3]|

    UNWIND b as c
    */
    auto *exprList = new ExpressionList(3);
    exprList->add(new ConstantExpression(1));
    exprList->add(new ConstantExpression(2));
    exprList->add(new ConstantExpression(3));

    auto *col1 = new YieldColumn(new ConstantExpression(1), new std::string("a"));
    auto *col2 = new YieldColumn(new ListExpression(exprList), new std::string("b"));
    auto *columns = qctx_->objPool()->add(new YieldColumns());
    columns->addColumn(col1);
    columns->addColumn(col2);
    auto *project = Project::make(qctx_.get(), start_, columns);
    project->setColNames({"a", "b"});
    auto proExe = Executor::create(project, qctx_.get());
    EXPECT_TRUE(proExe->execute().get().ok());
    auto &proResult = qctx_->ectx()->getResult(project->outputVar());
    DataSet expected;
    expected.colNames = {"a", "b"};
    Row row;
    row.values.emplace_back(1);
    row.values.emplace_back(List({1, 2, 3}));
    expected.rows.emplace_back(std::move(row));
    EXPECT_EQ(proResult.value().getDataSet(), expected);
    EXPECT_EQ(proResult.state(), Result::State::kSuccess);

    auto col =
        new YieldColumn(new VariablePropertyExpression(new std::string(), new std::string("b")));
    auto *columns2 = new YieldColumns();
    qctx_->objPool()->add(columns2);
    columns2->addColumn(col);

    auto *unwind = Unwind::make(qctx_.get(), project, columns2);
    unwind->setColNames(std::vector<std::string>{"c"});

    auto unwExe = Executor::create(unwind, qctx_.get());
    auto future = unwExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto &unwindResult = qctx_->ectx()->getResult(unwind->outputVar());

    DataSet expected2;
    expected2.colNames = {"c"};
    for (auto i = 1; i <= 3; ++i) {
        Row row2;
        row2.values.emplace_back(i);
        expected2.rows.emplace_back(std::move(row2));
    }
    EXPECT_EQ(unwindResult.value().getDataSet(), expected2);
    EXPECT_EQ(unwindResult.state(), Result::State::kSuccess);
}

}   // namespace graph
}   // namespace nebula
