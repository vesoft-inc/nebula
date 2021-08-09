/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/plan/Query.h"
#include "executor/query/InnerJoinExecutor.h"
#include "executor/query/LeftJoinExecutor.h"
#include "executor/test/QueryTestBase.h"

namespace nebula {
namespace graph {
class JoinTest : public QueryTestBase {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        pool_ = qctx_->objPool();

        {
            DataSet ds;
            ds.colNames = {kVid, "tag_prop", "edge_prop", kDst};
            for (auto i = 0; i < 10; ++i) {
                Row row;
                // _vid
                row.values.emplace_back(folly::to<std::string>(i / 2));
                row.values.emplace_back(i);
                row.values.emplace_back(i + 1);
                // _dst
                row.values.emplace_back(folly::to<std::string>(i / 2 + 5 + i % 2));
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->symTable()->newVariable("var1");
            qctx_->ectx()->setResult(
                "var1",
                ResultBuilder().value(Value(std::move(ds))).finish());
        }
        {
            DataSet ds;
            ds.colNames = {"src", "dst"};
            for (auto i = 0; i < 5; ++i) {
                Row row;
                row.values.emplace_back(folly::to<std::string>(i + 11));
                row.values.emplace_back(folly::to<std::string>(i));
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->symTable()->newVariable("var2");
            qctx_->ectx()->setResult(
                "var2", ResultBuilder().value(Value(std::move(ds))).finish());
        }
        {
            DataSet ds;
            ds.colNames = {"col1"};
            Row row;
            row.values.emplace_back("11");
            ds.rows.emplace_back(std::move(row));
            qctx_->symTable()->newVariable("var3");
            qctx_->ectx()->setResult(
                "var3", ResultBuilder().value(Value(std::move(ds))).finish());
        }
        {
            DataSet ds;
            ds.colNames = {kVid, "tag_prop", "edge_prop", kDst};
            qctx_->symTable()->newVariable("empty_var1");
            qctx_->ectx()->setResult(
                "empty_var1",
                ResultBuilder().value(Value(std::move(ds))).finish());
        }
        {
            DataSet ds;
            ds.colNames = {"src", "dst"};
            qctx_->symTable()->newVariable("empty_var2");
            qctx_->ectx()->setResult(
                "empty_var2",
                ResultBuilder().value(Value(std::move(ds))).finish());
        }
    }

    void testInnerJoin(std::string left, std::string right, DataSet& expected, int64_t line);
    void testLeftJoin(std::string left, std::string right, DataSet& expected, int64_t line);

protected:
    std::unique_ptr<QueryContext> qctx_;
    ObjectPool* pool_;
};

void JoinTest::testInnerJoin(std::string left, std::string right,
                            DataSet& expected, int64_t line) {
    auto key = VariablePropertyExpression::make(pool_, left, "dst");
    std::vector<Expression*> hashKeys = {key};
    auto probe = VariablePropertyExpression::make(pool_, right, "_vid");
    std::vector<Expression*> probeKeys = {probe};

    auto* join =
        InnerJoin::make(qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys),
                       std::move(probeKeys));
    join->setColNames(std::vector<std::string>{
        "src", "dst", kVid, "tag_prop", "edge_prop", kDst});

    auto joinExe =
        std::make_unique<InnerJoinExecutor>(join, qctx_.get());
    auto future = joinExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok()) << "LINE: " << line;
    auto& result = qctx_->ectx()->getResult(join->outputVar());

    DataSet resultDs;
    resultDs.colNames = {
        "src", "dst", kVid, "tag_prop", "edge_prop", kDst};
    auto iter = result.iter();
    for (; iter->valid(); iter->next()) {
        const auto& cols = *iter->row();
        Row row;
        for (size_t i = 0; i < cols.size(); ++i) {
            Value col = cols[i];
            row.values.emplace_back(std::move(col));
        }
        resultDs.rows.emplace_back(std::move(row));
    }

    EXPECT_EQ(resultDs, expected) << "LINE: " << line;
    EXPECT_EQ(result.state(), Result::State::kSuccess) << "LINE: " << line;
}

void JoinTest::testLeftJoin(std::string left, std::string right,
                            DataSet& expected, int64_t line) {
    auto key = VariablePropertyExpression::make(pool_, left, "_vid");
    std::vector<Expression*> hashKeys = {key};
    auto probe = VariablePropertyExpression::make(pool_, right, "dst");
    std::vector<Expression*> probeKeys = {probe};

    auto* join =
        LeftJoin::make(qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys),
                       std::move(probeKeys));
    join->setColNames(std::vector<std::string>{
        kVid, "tag_prop", "edge_prop", kDst, "src", "dst"});

    auto joinExe =
        std::make_unique<LeftJoinExecutor>(join, qctx_.get());
    auto future = joinExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok()) << "LINE: " << line;
    auto& result = qctx_->ectx()->getResult(join->outputVar());

    DataSet resultDs;
    resultDs.colNames = {
        kVid, "tag_prop", "edge_prop", kDst, "src", "dst"};
    auto iter = result.iter();
    for (; iter->valid(); iter->next()) {
        const auto& cols = *iter->row();
        Row row;
        for (size_t i = 0; i < cols.size(); ++i) {
            Value col = cols[i];
            row.values.emplace_back(std::move(col));
        }
        resultDs.rows.emplace_back(std::move(row));
    }

    EXPECT_EQ(resultDs, expected) << "LINE: " << line;
    EXPECT_EQ(result.state(), Result::State::kSuccess) << "LINE: " << line;
}

TEST_F(JoinTest, InnerJoin) {
    DataSet expected;
    expected.colNames = {
        "src", "dst", kVid, "tag_prop", "edge_prop", kDst};
    for (auto i = 11; i < 16; ++i) {
        Row row1;
        row1.values.emplace_back(folly::to<std::string>(i));
        row1.values.emplace_back(folly::to<std::string>(i % 11));
        row1.values.emplace_back(folly::to<std::string>(i % 11));
        row1.values.emplace_back(i % 11 * 2);
        row1.values.emplace_back(i % 11 * 2 + 1);
        row1.values.emplace_back(folly::to<std::string>(i - 6));
        expected.rows.emplace_back(std::move(row1));

        Row row2;
        row2.values.emplace_back(folly::to<std::string>(i));
        row2.values.emplace_back(folly::to<std::string>(i % 11));
        row2.values.emplace_back(folly::to<std::string>(i % 11));
        row2.values.emplace_back(i % 11 * 2 + 1);
        row2.values.emplace_back(i % 11 * 2 + 2);
        row2.values.emplace_back(folly::to<std::string>(i - 5));
        expected.rows.emplace_back(std::move(row2));
    }

    // $var1 inner join $var2 on $var2.dst = $var1._vid
    testInnerJoin("var2", "var1", expected, __LINE__);
}

TEST_F(JoinTest, InnerJoinTwice) {
    std::string joinOutput;
    {
        std::string left = "var2";
        std::string right = "var1";
        auto key = VariablePropertyExpression::make(pool_, left, "dst");
        std::vector<Expression*> hashKeys = {key};
        auto probe = VariablePropertyExpression::make(pool_, right, "_vid");
        std::vector<Expression*> probeKeys = {probe};

        auto* join =
            InnerJoin::make(qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys),
                        std::move(probeKeys));
        join->setColNames(
            std::vector<std::string>{"src", "dst", kVid, "tag_prop", "edge_prop", kDst});

        auto joinExe =
            std::make_unique<InnerJoinExecutor>(join, qctx_.get());
        auto future = joinExe->execute();

        joinOutput = join->outputVar();
    }

    std::string left = joinOutput;
    std::string right = "var3";
    auto key = VariablePropertyExpression::make(pool_, left, "src");
    std::vector<Expression*> hashKeys = {key};
    auto probe = VariablePropertyExpression::make(pool_, right, "col1");
    std::vector<Expression*> probeKeys = {probe};

    auto* join =
        InnerJoin::make(qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys),
                    std::move(probeKeys));
    join->setColNames(std::vector<std::string>{
        "src", "dst", kVid, "tag_prop", "edge_prop", kDst, "col1"});

    auto joinExe =
        std::make_unique<InnerJoinExecutor>(join, qctx_.get());
    auto future = joinExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(join->outputVar());

    DataSet resultDs;
    resultDs.colNames = {
        "src", "dst", kVid, "tag_prop", "edge_prop", kDst, "col1"};
    auto iter = result.iter();
    for (; iter->valid(); iter->next()) {
        const auto& cols = *iter->row();
        Row row;
        for (size_t i = 0; i < cols.size(); ++i) {
            Value col = cols[i];
            row.values.emplace_back(std::move(col));
        }
        resultDs.rows.emplace_back(std::move(row));
    }

    DataSet expected;
    expected.colNames = {
        "src", "dst", kVid, "tag_prop", "edge_prop", kDst, "col1"};
    Row row1;
    row1.values.emplace_back("11");
    row1.values.emplace_back("0");
    row1.values.emplace_back("0");
    row1.values.emplace_back(0);
    row1.values.emplace_back(1);
    row1.values.emplace_back("5");
    row1.values.emplace_back("11");
    expected.rows.emplace_back(row1);
    row1.values[3] = 1;
    row1.values[4] = 2;
    row1.values[5] = "6";
    expected.rows.emplace_back(std::move(row1));

    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(JoinTest, InnerJoinEmpty) {
    {
        DataSet expected;
        expected.colNames = {
            "src", "dst", kVid, "tag_prop", "edge_prop", kDst};

        testInnerJoin("var2", "empty_var1", expected, __LINE__);
    }

    {
        DataSet expected;
        expected.colNames = {
            "src", "dst", kVid, "tag_prop", "edge_prop", kDst};

        testInnerJoin("empty_var2", "var1", expected, __LINE__);
    }

    {
        DataSet expected;
        expected.colNames = {
            "src", "dst", kVid, "tag_prop", "edge_prop", kDst};

        testInnerJoin("empty_var2", "empty_var1", expected, __LINE__);
    }
}

TEST_F(JoinTest, LeftJoin) {
    DataSet expected;
    expected.colNames = {kVid, "tag_prop", "edge_prop", kDst, "src", "dst"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.values.emplace_back(folly::to<std::string>(i / 2));
        row.values.emplace_back(i);
        row.values.emplace_back(i + 1);
        row.values.emplace_back(folly::to<std::string>(i / 2 + 5 + i % 2));
        row.values.emplace_back(folly::to<std::string>(i / 2 + 11));
        row.values.emplace_back(folly::to<std::string>(i / 2));
        expected.rows.emplace_back(std::move(row));
    }

    // $var1 left join $var2 on $var1._vid = $var2.dst
    testLeftJoin("var1", "var2", expected, __LINE__);
}

TEST_F(JoinTest, LeftJoinTwice) {
    std::string joinOutput;
    {
        std::string left = "var1";
        std::string right = "var2";
        auto key = VariablePropertyExpression::make(pool_, left, "_vid");
        std::vector<Expression*> hashKeys = {key};
        auto probe = VariablePropertyExpression::make(pool_, right, "dst");
        std::vector<Expression*> probeKeys = {probe};

        auto* join = LeftJoin::make(
            qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys), std::move(probeKeys));
        join->setColNames(
            std::vector<std::string>{kVid, "tag_prop", "edge_prop", kDst, "src", "dst"});

        auto joinExe = std::make_unique<LeftJoinExecutor>(join, qctx_.get());
        auto future = joinExe->execute();

        joinOutput = join->outputVar();
    }

    std::string left = joinOutput;
    std::string right = "var3";
    auto key = VariablePropertyExpression::make(pool_, left, "src");
    std::vector<Expression*> hashKeys = {key};
    auto probe = VariablePropertyExpression::make(pool_, right, "col1");
    std::vector<Expression*> probeKeys = {probe};

    auto* join = LeftJoin::make(
        qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys), std::move(probeKeys));
    join->setColNames(
        std::vector<std::string>{kVid, "tag_prop", "edge_prop", kDst, "src", "dst", "col1"});

    auto joinExe = std::make_unique<LeftJoinExecutor>(join, qctx_.get());
    auto future = joinExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(join->outputVar());

    DataSet resultDs;
    resultDs.colNames = {kVid, "tag_prop", "edge_prop", kDst, "src", "dst", "col1"};
    auto iter = result.iter();
    for (; iter->valid(); iter->next()) {
        const auto& cols = *iter->row();
        Row row;
        for (size_t i = 0; i < cols.size(); ++i) {
            Value col = cols[i];
            row.values.emplace_back(std::move(col));
        }
        resultDs.rows.emplace_back(std::move(row));
    }

    DataSet expected;
    expected.colNames = {kVid, "tag_prop", "edge_prop", kDst, "src", "dst", "col1"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.values.emplace_back(folly::to<std::string>(i / 2));
        row.values.emplace_back(i);
        row.values.emplace_back(i + 1);
        row.values.emplace_back(folly::to<std::string>(i / 2 + 5 + i % 2));
        row.values.emplace_back(folly::to<std::string>(i / 2 + 11));
        row.values.emplace_back(folly::to<std::string>(i / 2));
        if (i < 2) {
            row.values.emplace_back(folly::to<std::string>(11));
        } else {
            row.values.emplace_back(Value::kEmpty);
        }
        expected.rows.emplace_back(std::move(row));
    }
    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(JoinTest, LeftJoinEmpty) {
    {
        DataSet expected;
        expected.colNames = {kVid, "tag_prop", "edge_prop", kDst, "src", "dst"};
        for (auto i = 0; i < 10; ++i) {
            Row row;
            row.values.emplace_back(folly::to<std::string>(i / 2));
            row.values.emplace_back(i);
            row.values.emplace_back(i + 1);
            row.values.emplace_back(folly::to<std::string>(i / 2 + 5 + i % 2));
            row.values.emplace_back(Value::kEmpty);
            row.values.emplace_back(Value::kEmpty);
            expected.rows.emplace_back(std::move(row));
        }
        testLeftJoin("var1", "empty_var2", expected, __LINE__);
    }

    {
        DataSet expected;
        expected.colNames = {kVid, "tag_prop", "edge_prop", kDst, "src", "dst"};
        testLeftJoin("empty_var1", "var2", expected, __LINE__);
    }

    {
        DataSet expected;
        expected.colNames = {kVid, "tag_prop", "edge_prop", kDst, "src", "dst"};
        testLeftJoin("empty_var1", "empty_var2", expected, __LINE__);
    }
}

TEST_F(JoinTest, LeftJoinAndInnerjoin) {
    std::string joinOutput;
    {
        std::string left = "var1";
        std::string right = "var2";
        auto key = VariablePropertyExpression::make(pool_, left, "_vid");
        std::vector<Expression*> hashKeys = {key};
        auto probe = VariablePropertyExpression::make(pool_, right, "dst");
        std::vector<Expression*> probeKeys = {probe};

        auto* join = LeftJoin::make(
            qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys), std::move(probeKeys));
        join->setColNames(
            std::vector<std::string>{kVid, "tag_prop", "edge_prop", kDst, "src", "dst"});

        auto joinExe = std::make_unique<LeftJoinExecutor>(join, qctx_.get());
        auto future = joinExe->execute();

        joinOutput = join->outputVar();
    }

    std::string left = joinOutput;
    std::string right = "var3";
    auto key = VariablePropertyExpression::make(pool_, left, "src");
    std::vector<Expression*> hashKeys = {key};
    auto probe = VariablePropertyExpression::make(pool_, right, "col1");
    std::vector<Expression*> probeKeys = {probe};

    auto* join = InnerJoin::make(
        qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys), std::move(probeKeys));
    join->setColNames(
        std::vector<std::string>{kVid, "tag_prop", "edge_prop", kDst, "src", "dst", "col1"});

    auto joinExe = std::make_unique<InnerJoinExecutor>(join, qctx_.get());
    auto future = joinExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(join->outputVar());

    DataSet resultDs;
    resultDs.colNames = {kVid, "tag_prop", "edge_prop", kDst, "src", "dst", "col1"};
    auto iter = result.iter();
    for (; iter->valid(); iter->next()) {
        const auto& cols = *iter->row();
        Row row;
        for (size_t i = 0; i < cols.size(); ++i) {
            Value col = cols[i];
            row.values.emplace_back(std::move(col));
        }
        resultDs.rows.emplace_back(std::move(row));
    }

    DataSet expected;
    expected.colNames = {kVid, "tag_prop", "edge_prop", kDst, "src", "dst", "col1"};
    for (auto i = 0; i < 2; ++i) {
        Row row;
        row.values.emplace_back(folly::to<std::string>(i / 2));
        row.values.emplace_back(i);
        row.values.emplace_back(i + 1);
        row.values.emplace_back(folly::to<std::string>(i / 2 + 5 + i % 2));
        row.values.emplace_back(folly::to<std::string>(i / 2 + 11));
        row.values.emplace_back(folly::to<std::string>(i / 2));
        row.values.emplace_back(folly::to<std::string>(11));
        expected.rows.emplace_back(std::move(row));
    }
    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(JoinTest, InnerJoinAndLeftjoin) {
    std::string joinOutput;
    {
        std::string left = "var1";
        std::string right = "var2";
        auto key = VariablePropertyExpression::make(pool_, left, "_vid");
        std::vector<Expression*> hashKeys = {key};
        auto probe = VariablePropertyExpression::make(pool_, right, "dst");
        std::vector<Expression*> probeKeys = {probe};

        auto* join = InnerJoin::make(
            qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys), std::move(probeKeys));
        join->setColNames(
            std::vector<std::string>{kVid, "tag_prop", "edge_prop", kDst, "src", "dst"});

        auto joinExe = std::make_unique<InnerJoinExecutor>(join, qctx_.get());
        auto future = joinExe->execute();

        joinOutput = join->outputVar();
    }

    std::string left = joinOutput;
    std::string right = "var3";
    auto key = VariablePropertyExpression::make(pool_, left, "src");
    std::vector<Expression*> hashKeys = {key};
    auto probe = VariablePropertyExpression::make(pool_, right, "col1");
    std::vector<Expression*> probeKeys = {probe};

    auto* join = LeftJoin::make(
        qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys), std::move(probeKeys));
    join->setColNames(
        std::vector<std::string>{kVid, "tag_prop", "edge_prop", kDst, "src", "dst", "col1"});

    auto joinExe = std::make_unique<LeftJoinExecutor>(join, qctx_.get());
    auto future = joinExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(join->outputVar());

    DataSet resultDs;
    resultDs.colNames = {kVid, "tag_prop", "edge_prop", kDst, "src", "dst", "col1"};
    auto iter = result.iter();
    for (; iter->valid(); iter->next()) {
        const auto& cols = *iter->row();
        Row row;
        for (size_t i = 0; i < cols.size(); ++i) {
            Value col = cols[i];
            row.values.emplace_back(std::move(col));
        }
        resultDs.rows.emplace_back(std::move(row));
    }

    DataSet expected;
    expected.colNames = {kVid, "tag_prop", "edge_prop", kDst, "src", "dst", "col1"};
    for (auto i = 0; i < 10; ++i) {
        Row row;
        row.values.emplace_back(folly::to<std::string>(i / 2));
        row.values.emplace_back(i);
        row.values.emplace_back(i + 1);
        row.values.emplace_back(folly::to<std::string>(i / 2 + 5 + i % 2));
        row.values.emplace_back(folly::to<std::string>(i / 2 + 11));
        row.values.emplace_back(folly::to<std::string>(i / 2));
        if (i < 2) {
            row.values.emplace_back(folly::to<std::string>(11));
        } else {
            row.values.emplace_back(Value::kEmpty);
        }
        expected.rows.emplace_back(std::move(row));
    }
    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

}   // namespace graph
}   // namespace nebula
