/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/Query.h"
#include "executor/query/DataJoinExecutor.h"
#include "executor/query/test/QueryTestBase.h"

namespace nebula {
namespace graph {
class DataJoinTest : public QueryTestBase {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
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
            qctx_->ectx()->setResult(
                "var2", ResultBuilder().value(Value(std::move(ds))).finish());
        }
        {
            DataSet ds;
            ds.colNames = {"col1"};
            Row row;
            row.values.emplace_back("11");
            ds.rows.emplace_back(std::move(row));
            qctx_->ectx()->setResult(
                "var3", ResultBuilder().value(Value(std::move(ds))).finish());
        }
        {
            DataSet ds;
            ds.colNames = {kVid, "tag_prop", "edge_prop", kDst};
            qctx_->ectx()->setResult(
                "empty_var1",
                ResultBuilder().value(Value(std::move(ds))).finish());
        }
        {
            DataSet ds;
            ds.colNames = {"src", "dst"};
            qctx_->ectx()->setResult(
                "empty_var2",
                ResultBuilder().value(Value(std::move(ds))).finish());
        }
    }

    void testJoin(std::string left, std::string right, DataSet& expected, int64_t line);

protected:
    std::unique_ptr<QueryContext> qctx_;
};

void DataJoinTest::testJoin(std::string left, std::string right,
                            DataSet& expected, int64_t line) {
    VariablePropertyExpression key(new std::string(left),
                                   new std::string("dst"));
    std::vector<Expression*> hashKeys = {&key};
    VariablePropertyExpression probe(new std::string(right),
                                     new std::string("_vid"));
    std::vector<Expression*> probeKeys = {&probe};

    auto* dataJoin =
        DataJoin::make(qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys),
                       std::move(probeKeys));
    dataJoin->setColNames(std::vector<std::string>{
        "src", "dst", kVid, "tag_prop", "edge_prop", kDst});

    auto dataJoinExe =
        std::make_unique<DataJoinExecutor>(dataJoin, qctx_.get());
    auto future = dataJoinExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok()) << "LINE: " << line;
    auto& result = qctx_->ectx()->getResult(dataJoin->varName());

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

TEST_F(DataJoinTest, Join) {
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
    testJoin("var2", "var1", expected, __LINE__);
}

TEST_F(DataJoinTest, JoinTwice) {
    std::string join;
    {
        std::string left = "var2";
        std::string right = "var1";
        VariablePropertyExpression key(new std::string(left),
                                    new std::string("dst"));
        std::vector<Expression*> hashKeys = {&key};
        VariablePropertyExpression probe(new std::string(right),
                                        new std::string("_vid"));
        std::vector<Expression*> probeKeys = {&probe};

        auto* dataJoin =
            DataJoin::make(qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys),
                        std::move(probeKeys));
        dataJoin->setColNames(std::vector<std::string>{
            "src", "dst", kVid, "tag_prop", "edge_prop", kDst});

        auto dataJoinExe =
            std::make_unique<DataJoinExecutor>(dataJoin, qctx_.get());
        auto future = dataJoinExe->execute();

        join = dataJoin->varName();
    }

    std::string left = join;
    std::string right = "var3";
    VariablePropertyExpression key(new std::string(left),
                                new std::string("src"));
    std::vector<Expression*> hashKeys = {&key};
    VariablePropertyExpression probe(new std::string(right),
                                    new std::string("col1"));
    std::vector<Expression*> probeKeys = {&probe};

    auto* dataJoin =
        DataJoin::make(qctx_.get(), nullptr, {left, 0}, {right, 0}, std::move(hashKeys),
                    std::move(probeKeys));
    dataJoin->setColNames(std::vector<std::string>{
        "src", "dst", kVid, "tag_prop", "edge_prop", kDst, "col1"});

    auto dataJoinExe =
        std::make_unique<DataJoinExecutor>(dataJoin, qctx_.get());
    auto future = dataJoinExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(dataJoin->varName());

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

TEST_F(DataJoinTest, JoinEmpty) {
    {
        DataSet expected;
        expected.colNames = {
            "src", "dst", kVid, "tag_prop", "edge_prop", kDst};

        testJoin("var2", "empty_var1", expected, __LINE__);
    }

    {
        DataSet expected;
        expected.colNames = {
            "src", "dst", kVid, "tag_prop", "edge_prop", kDst};

        testJoin("empty_var2", "var1", expected, __LINE__);
    }

    {
        DataSet expected;
        expected.colNames = {
            "src", "dst", kVid, "tag_prop", "edge_prop", kDst};

        testJoin("empty_var2", "empty_var1", expected, __LINE__);
    }
}
}  // namespace graph
}  // namespace nebula
