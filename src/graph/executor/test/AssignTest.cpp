/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/query/AssignExecutor.h"
#include "common/expression/VariableExpression.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

class AssignExecutorTest : public testing::Test {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        pool_ = qctx_->objPool();
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
    ObjectPool* pool_;
};

TEST_F(AssignExecutorTest, SingleExpression) {
    {
        std::string varName = "intVar";
        int val = 13;
        qctx_->symTable()->newVariable(varName);
        auto* expr = ConstantExpression::make(pool_, val);

        auto* assign = Assign::make(qctx_.get(), nullptr);
        assign->assignVar(varName, expr);
        auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
        auto future = assignExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());

        auto result = qctx_->ectx()->getValue(varName);
        EXPECT_TRUE(result.isInt());
        EXPECT_EQ(result.getInt(), val);
    }
    {
        std::string varName = "floatVar";
        float val = 1.234563726090;
        qctx_->symTable()->newVariable(varName);
        auto* expr = ConstantExpression::make(pool_, val);

        auto* assign = Assign::make(qctx_.get(), nullptr);
        assign->assignVar(varName, expr);
        auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
        auto future = assignExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());

        auto result = qctx_->ectx()->getValue(varName);
        EXPECT_TRUE(result.isFloat());
        EXPECT_EQ(result.getFloat(), val);
    }
    {
        std::string varName = "stringVar";
        std::string val = "hello world";
        qctx_->symTable()->newVariable(varName);
        auto* expr = ConstantExpression::make(pool_, val);

        auto* assign = Assign::make(qctx_.get(), nullptr);
        assign->assignVar(varName, expr);
        auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
        auto future = assignExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());

        auto result = qctx_->ectx()->getValue(varName);
        EXPECT_TRUE(result.isStr());
        EXPECT_EQ(result.getStr(), val);
    }
}

TEST_F(AssignExecutorTest, MultiExpression) {
    std::string intVar = "intVar";
    std::string floatVar = "floatVar";
    std::string stringVar = "stringVar";
    qctx_->symTable()->newVariable(intVar);
    qctx_->symTable()->newVariable(floatVar);
    qctx_->symTable()->newVariable(stringVar);
    auto* assign = Assign::make(qctx_.get(), nullptr);

    assign->assignVar(intVar, ConstantExpression::make(pool_, 1));
    assign->assignVar(floatVar, ConstantExpression::make(pool_, 3.2425787));
    assign->assignVar(stringVar, ConstantExpression::make(pool_, "hello"));

    auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
    auto future = assignExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());

    auto result = qctx_->ectx()->getValue(floatVar);
    EXPECT_TRUE(result.isFloat());
    EXPECT_EQ(result.getFloat(), 3.2425787);

    result = qctx_->ectx()->getValue(stringVar);
    EXPECT_TRUE(result.isStr());
    EXPECT_EQ(result.getStr(), "hello");

    result = qctx_->ectx()->getValue(intVar);
    EXPECT_TRUE(result.isInt());
    EXPECT_EQ(result.getInt(), 1);
}


TEST_F(AssignExecutorTest, VariableExpression) {
    // var1 = 13, var = var1 + 7
    std::string varName = "var";
    qctx_->symTable()->newVariable(varName);

    std::string varName1 = "var1";
    qctx_->symTable()->newVariable(varName1);
    {
        // var1 = 13
        int val = 13;
        auto* expr = ConstantExpression::make(pool_, val);
        auto* assign = Assign::make(qctx_.get(), nullptr);
        assign->assignVar(varName1, expr);
        auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
        auto future = assignExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto result = qctx_->ectx()->getValue(varName1);
        EXPECT_TRUE(result.isInt());
        EXPECT_EQ(result.getInt(), val);
    }

    auto* addExpr = ArithmeticExpression::makeAdd(
        pool_, VariableExpression::make(pool_, varName1), ConstantExpression::make(pool_, 7));
    auto* assign = Assign::make(qctx_.get(), nullptr);
    assign->assignVar(varName, addExpr);
    auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
    auto future = assignExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto result = qctx_->ectx()->getValue(varName);
    EXPECT_TRUE(result.isInt());
    EXPECT_EQ(result.getInt(), 20);
}

TEST_F(AssignExecutorTest, VariableExpression1) {
    // var1 = 13, var = var1++
    std::string varName = "var";
    qctx_->symTable()->newVariable(varName);

    std::string varName1 = "var1";
    qctx_->symTable()->newVariable(varName1);
    {
        // var1 = 13
        int val = 13;
        auto* expr = ConstantExpression::make(pool_, val);
        auto* assign = Assign::make(qctx_.get(), nullptr);
        assign->assignVar(varName1, expr);
        auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
        auto future = assignExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto result = qctx_->ectx()->getValue(varName1);
        EXPECT_TRUE(result.isInt());
        EXPECT_EQ(result.getInt(), val);
    }
    // var = var1++
    auto* addExpr = UnaryExpression::makeIncr(pool_, VariableExpression::make(pool_, varName1));
    auto* assign = Assign::make(qctx_.get(), nullptr);
    assign->assignVar(varName, addExpr);
    auto assignExe = std::make_unique<AssignExecutor>(assign, qctx_.get());
    auto future = assignExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto result = qctx_->ectx()->getValue(varName);
    EXPECT_TRUE(result.isInt());
    EXPECT_EQ(result.getInt(), 14);
}

}   // namespace graph
}   // namespace nebula
