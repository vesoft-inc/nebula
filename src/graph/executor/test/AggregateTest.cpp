/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/query/AggregateExecutor.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {
class AggregateTest : public testing::Test {
protected:
    static void SetUpTestCase() {
        // ======================
        // | col1 | col2 | col3 |
        // ----------------------
        // |  0   |  0   |  0   |
        // ----------------------
        // |  1   |  0   |  0   |
        // ----------------------
        // |  2   |  1   |  0   |
        // ----------------------
        // |  3   |  1   |  0   |
        // ----------------------
        // |  4   |  2   |  1   |
        // ----------------------
        // |  5   |  2   |  1   |
        // ----------------------
        // |  6   |  3   |  1   |
        // ----------------------
        // |  7   |  3   |  1   |
        // ----------------------
        // |  8   |  4   |  2   |
        // ----------------------
        // |  9   |  4   |  2   |
        // ----------------------
        input_ = std::make_unique<std::string>("input_agg");
        qctx_ = std::make_unique<QueryContext>();
        pool_ = qctx_->objPool();

        DataSet ds;
        ds.colNames = {"col1", "col2", "col3"};
        for (auto i = 0; i < 10; ++i) {
            Row row;
            row.values.emplace_back(i);
            int64_t col2 = i / 2;
            row.values.emplace_back(col2);
            int64_t col3 = i / 4;
            row.values.emplace_back(col3);
            ds.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        ds.rows.emplace_back(std::move(row));

        qctx_->symTable()->newVariable(*input_);
        qctx_->ectx()->setResult(*input_, ResultBuilder().value(Value(ds)).finish());
    }

protected:
    static std::unique_ptr<QueryContext> qctx_;
    static std::unique_ptr<std::string> input_;
    static ObjectPool* pool_;
};

std::unique_ptr<QueryContext> AggregateTest::qctx_;
std::unique_ptr<std::string> AggregateTest::input_;
ObjectPool* AggregateTest::pool_;

struct RowCmp {
    bool operator()(const Row& lhs, const Row& rhs) {
        DCHECK_EQ(lhs.values.size(), rhs.values.size());
        for (size_t i = 0; i < lhs.values.size(); ++i) {
            if (lhs.values[i] == rhs.values[i]) {
                continue;
            } else {
                return lhs.values[i] < rhs.values[i];
            }
        }
        return false;
    }
};

#define TEST_AGG_1(FUN, COL, DISTINCT)                                                             \
    std::vector<Expression*> groupKeys;                                                            \
    std::vector<Expression*> groupItems;                                                           \
    auto expr = InputPropertyExpression::make(pool_, "col1");                                      \
    auto item = AggregateExpression::make(pool_, (FUN), expr, DISTINCT);                           \
    groupItems.emplace_back(item);                                                                 \
    auto* agg =                                                                                    \
        Aggregate::make(qctx_.get(), nullptr, std::move(groupKeys), std::move(groupItems));        \
    agg->setInputVar(*input_);                                                                     \
    agg->setColNames(std::vector<std::string>{COL});                                               \
                                                                                                   \
    auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());                           \
    auto future = aggExe->execute();                                                               \
    auto status = std::move(future).get();                                                         \
    EXPECT_TRUE(status.ok());                                                                      \
    auto& result = qctx_->ectx()->getResult(agg->outputVar());                                     \
    EXPECT_EQ(result.value().getDataSet(), expected);                                              \
    EXPECT_EQ(result.state(), Result::State::kSuccess);

#define TEST_AGG_2(FUN, COL, DISTINCT)                                                             \
    std::vector<Expression*> groupKeys;                                                            \
    std::vector<Expression*> groupItems;                                                           \
    auto expr1 = InputPropertyExpression::make(pool_, "col2");                                     \
    auto expr2 = expr1->clone();                                                                   \
    groupKeys.emplace_back(expr1);                                                                 \
    auto item = AggregateExpression::make(pool_, (FUN), expr2, DISTINCT);                          \
    groupItems.emplace_back(item);                                                                 \
    auto* agg =                                                                                    \
        Aggregate::make(qctx_.get(), nullptr, std::move(groupKeys), std::move(groupItems));        \
    agg->setInputVar(*input_);                                                                     \
    agg->setColNames(std::vector<std::string>{COL});                                               \
                                                                                                   \
    auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());                           \
    auto future = aggExe->execute();                                                               \
    auto status = std::move(future).get();                                                         \
    EXPECT_TRUE(status.ok());                                                                      \
    auto& result = qctx_->ectx()->getResult(agg->outputVar());                                     \
    DataSet sortedDs = result.value().getDataSet();                                                \
    std::sort(sortedDs.rows.begin(), sortedDs.rows.end(), RowCmp());                               \
    EXPECT_EQ(sortedDs, expected);                                                                 \
    EXPECT_EQ(result.state(), Result::State::kSuccess);

#define TEST_AGG_3(FUN, COL, DISTINCT)                                                             \
    std::vector<Expression*> groupKeys;                                                            \
    std::vector<Expression*> groupItems;                                                           \
    auto expr1 = InputPropertyExpression::make(pool_, "col2");                                     \
    auto expr2 = expr1->clone();                                                                   \
    groupKeys.emplace_back(expr1);                                                                 \
    auto item = AggregateExpression::make(pool_, "", expr2, false);                                \
    groupItems.emplace_back(item);                                                                 \
    auto expr3 = InputPropertyExpression::make(pool_, "col3");                                     \
    groupKeys.emplace_back(expr3);                                                                 \
    auto item1 = AggregateExpression::make(pool_, (FUN), expr3, DISTINCT);                         \
    groupItems.emplace_back(item1);                                                               \
    auto* agg =                                                                                    \
        Aggregate::make(qctx_.get(), nullptr, std::move(groupKeys), std::move(groupItems));        \
    agg->setInputVar(*input_);                                                                     \
    agg->setColNames(std::vector<std::string>{"col2", COL});                                       \
                                                                                                   \
    auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());                           \
    auto future = aggExe->execute();                                                               \
    auto status = std::move(future).get();                                                         \
    EXPECT_TRUE(status.ok());                                                                      \
    auto& result = qctx_->ectx()->getResult(agg->outputVar());                                     \
    DataSet sortedDs = result.value().getDataSet();                                                \
    std::sort(sortedDs.rows.begin(), sortedDs.rows.end(), RowCmp());                               \
    EXPECT_EQ(sortedDs, expected);                                                                 \
    EXPECT_EQ(result.state(), Result::State::kSuccess);

#define TEST_AGG_4(FUN, COL, DISTINCT)                                                             \
    std::vector<Expression*> groupKeys;                                                            \
    std::vector<Expression*> groupItems;                                                           \
    auto expr = ConstantExpression::make(pool_, 1);                                                \
    auto item = AggregateExpression::make(pool_, (FUN), expr, DISTINCT);                           \
    groupItems.emplace_back(item);                                                                 \
    auto* agg =                                                                                    \
        Aggregate::make(qctx_.get(), nullptr, std::move(groupKeys), std::move(groupItems));        \
    agg->setInputVar(*input_);                                                                     \
    agg->setColNames(std::vector<std::string>{COL});                                               \
                                                                                                   \
    auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());                           \
    auto future = aggExe->execute();                                                               \
    auto status = std::move(future).get();                                                         \
    EXPECT_TRUE(status.ok());                                                                      \
    auto& result = qctx_->ectx()->getResult(agg->outputVar());                                     \
    EXPECT_EQ(result.value().getDataSet(), expected);                                              \
    EXPECT_EQ(result.state(), Result::State::kSuccess);

TEST_F(AggregateTest, Group) {
    {
        // ========
        // | col2 |
        // --------
        // |  0   |
        // --------
        // |  1   |
        // --------
        // |  2   |
        // --------
        // |  3   |
        // --------
        // |  4   |
        // --------
        // | NULL |
        // --------
        DataSet expected;
        expected.colNames = {"col2"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2
        // items = col2
        TEST_AGG_2("", "col2", false)
    }
    {
        // ===============
        // | col2 | col3 |
        // ---------------
        // |  0   |  0   |
        // ---------------
        // |  1   |  0   |
        // ---------------
        // |  2   |  1   |
        // ---------------
        // |  3   |  1   |
        // ---------------
        // |  4   |  2   |
        // ---------------
        // | NULL | NULL |
        // ---------------
        DataSet expected;
        expected.colNames = {"col2", "col3"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, col3
        TEST_AGG_3("", "col3", false)
    }
}

TEST_F(AggregateTest, Collect) {
    {
        // ====================================
        // | list                             |
        // ------------------------------------
        // | [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]   |
        // ------------------------------------
        DataSet expected;
        expected.colNames = {"list"};
        Row row;
        List list;
        for (auto i = 0; i < 10; ++i) {
            list.values.emplace_back(i);
        }
        row.emplace_back(std::move(list));
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = collect(col1)
        TEST_AGG_1("COLLECT", "list", false)
    }
    {
        // ========
        // | list |
        // --------
        // | [ 0 ]|
        // --------
        // | [ 1 ]|
        // --------
        // | [ 2 ]|
        // --------
        // | [ 3 ]|
        // --------
        // | [ 4 ]|
        // --------
        // | [ 5 ]|
        // --------
        // | [ 6 ]|
        // --------
        // | [ 7 ]|
        // --------
        // | [ 8 ]|
        // --------
        // | [ 9 ]|
        // --------
        // | []   |
        // --------
        DataSet expected;
        expected.colNames = {"list"};
        for (auto i = 0; i < 10; ++i) {
            Row row;
            List list;
            list.values.emplace_back(i);
            row.values.emplace_back(std::move(list));
            expected.rows.emplace_back(std::move(row));
        }

        // key = col1
        // items = collect(col1)
        std::vector<Expression*> groupKeys;
        std::vector<Expression*> groupItems;
        auto expr1 = InputPropertyExpression::make(pool_, "col1");
        auto expr2 = expr1->clone();
        groupKeys.emplace_back(expr1);
        auto item = AggregateExpression::make(pool_, "COLLECT", expr2, false);
        groupItems.emplace_back(item);
        auto* agg =
            Aggregate::make(qctx_.get(), nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"list"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->outputVar());
        auto& ds = result.value().getDataSet();
        std::vector<Value> vals;
        for (auto& r : ds.rows) {
            for (auto& c : r.values) {
                EXPECT_EQ(c.type(), Value::Type::LIST);
                auto& list = c.getList();
                for (auto& v : list.values) {
                    vals.emplace_back(std::move(v));
                }
            }
        }
        std::vector<Value> expectedVals = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        std::sort(vals.begin(), vals.end());
        EXPECT_EQ(vals, expectedVals);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
    {
        // ===================
        // | col2 | list     |
        // -------------------
        // |  0   | [ 0, 0 ] |
        // -------------------
        // |  1   | [ 0, 0 ] |
        // -------------------
        // |  2   | [ 1, 1 ] |
        // -------------------
        // |  3   | [ 1, 1 ] |
        // -------------------
        // |  4   | [ 2, 2 ] |
        // -------------------
        // | NULL | []       |
        // -------------------
        DataSet expected;
        expected.colNames = {"col2", "list"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            List list;
            list.values = {i / 2, i / 2};
            row.values.emplace_back(i);
            row.values.emplace_back(std::move(list));
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        List list;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(std::move(list));
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, collect(col3)
        TEST_AGG_3("COLLECT", "list", false)
    }
    {
        // ====================================
        // | list                             |
        // ------------------------------------
        // | [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]|
        // ------------------------------------
        DataSet expected;
        expected.colNames = {"list"};
        Row row;
        List list;
        for (auto i = 0; i < 11; ++i) {
            list.values.emplace_back(1);
        }
        row.emplace_back(std::move(list));
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = collect(1)
        TEST_AGG_4("COLLECT", "list", false)
    }
    {
        // ====================================
        // | list                             |
        // ------------------------------------
        // | [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]   |
        // ------------------------------------
        DataSet expected;
        expected.colNames = {"list"};
        Row row;
        List list;
        for (auto i = 0; i < 10; ++i) {
            list.values.emplace_back(i);
        }
        row.emplace_back(std::move(list));
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = collect(distinct col1)
        std::vector<Expression*> groupKeys;
        std::vector<Expression*> groupItems;
        auto expr = InputPropertyExpression::make(pool_, "col1");
        auto item = AggregateExpression::make(pool_, "COLLECT", expr, true);
        groupItems.emplace_back(item);
        auto* agg =
            Aggregate::make(qctx_.get(), nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"list"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->outputVar());
        EXPECT_EQ(result.value().getDataSet().rows.size(), 1);
        EXPECT_EQ(result.value().getDataSet().rows[0].values[0].isList(), true);
        auto resultList = result.value().getDataSet().rows[0].values[0].getList();
        std::sort(resultList.values.begin(), resultList.values.end());
        EXPECT_EQ(resultList, expected.rows[0].values[0].getList());
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
    {
        // ========
        // | list |
        // --------
        // | [ 0 ]|
        // --------
        // | [ 1 ]|
        // --------
        // | [ 2 ]|
        // --------
        // | [ 3 ]|
        // --------
        // | [ 4 ]|
        // --------
        // | [ 5 ]|
        // --------
        // | [ 6 ]|
        // --------
        // | [ 7 ]|
        // --------
        // | [ 8 ]|
        // --------
        // | [ 9 ]|
        // --------
        // | []   |
        // --------
        DataSet expected;
        expected.colNames = {"list"};
        for (auto i = 0; i < 10; ++i) {
            Row row;
            List list;
            list.values.emplace_back(i);
            row.values.emplace_back(std::move(list));
            expected.rows.emplace_back(std::move(row));
        }

        // key = col1
        // items = collect(distinct col1)
        std::vector<Expression*> groupKeys;
        std::vector<Expression*> groupItems;
        auto expr1 = InputPropertyExpression::make(pool_, "col1");
        auto expr2 = expr1->clone();
        auto item = AggregateExpression::make(pool_, "COLLECT", expr2, true);
        groupItems.emplace_back(item);
        auto* agg =
            Aggregate::make(qctx_.get(), nullptr, std::move(groupKeys), std::move(groupItems));
        agg->setInputVar(*input_);
        agg->setColNames(std::vector<std::string>{"list"});

        auto aggExe = std::make_unique<AggregateExecutor>(agg, qctx_.get());
        auto future = aggExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(agg->outputVar());
        auto& ds = result.value().getDataSet();
        std::vector<Value> vals;
        for (auto& r : ds.rows) {
            for (auto& c : r.values) {
                EXPECT_EQ(c.type(), Value::Type::LIST);
                auto& list = c.getList();
                for (auto& v : list.values) {
                    vals.emplace_back(std::move(v));
                }
            }
        }
        std::vector<Value> expectedVals = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
        std::sort(vals.begin(), vals.end());
        EXPECT_EQ(vals, expectedVals);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
    {
        // ===================
        // | col2 | list     |
        // -------------------
        // |  0   | [ 0 ]    |
        // -------------------
        // |  1   | [ 0 ]    |
        // -------------------
        // |  2   | [ 1 ]    |
        // -------------------
        // |  3   | [ 1 ]    |
        // -------------------
        // |  4   | [ 2 ]    |
        // -------------------
        // | NULL | []       |
        // -------------------
        DataSet expected;
        expected.colNames = {"col2", "list"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            List list;
            list.values = {i / 2};
            row.values.emplace_back(i);
            row.values.emplace_back(std::move(list));
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        List list;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(std::move(list));
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, collect(distinct col3)

        TEST_AGG_3("COLLECT", "list", true)
    }
    {
        // ====================================
        // | list                             |
        // ------------------------------------
        // | [ 1 ]                            |
        // ------------------------------------
        DataSet expected;
        expected.colNames = {"list"};
        Row row;
        List list;
        list.values.emplace_back(1);
        row.emplace_back(std::move(list));
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = collect(distinct 1)
        TEST_AGG_4("COLLECT", "list", true)
    }
}

TEST_F(AggregateTest, Count) {
    {
        // ========
        // | count|
        // --------
        // | 10   |
        // --------
        DataSet expected;
        expected.colNames = {"count"};
        Row row;
        row.emplace_back(10);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = count(col1)
        TEST_AGG_1("COUNT", "count", false)
    }
    {
        // ========
        // | count|
        // --------
        // |   0  |
        // --------
        // |   2  |
        // --------
        // |   2  |
        // --------
        // |   2  |
        // --------
        // |   2  |
        // --------
        // |   2  |
        // -------
        DataSet expected;
        expected.colNames = {"count"};
        {
            Row row;
            row.values.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(2);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = count(col2)
        TEST_AGG_2("COUNT", "count", false)
    }
    {
        // ================
        // | col2 | count |
        // ----------------
        // |  0   |   2   |
        // ----------------
        // |  1   |   2   |
        // ----------------
        // |  2   |   2   |
        // ----------------
        // |  3   |   2   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        // | NULL |   0   |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "count"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, count(col3)
        TEST_AGG_3("COUNT", "count", false)
    }
    {
        // ========
        // | count|
        // --------
        // | 11   |
        DataSet expected;
        expected.colNames = {"count"};
        Row row;
        row.emplace_back(11);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = count(1)
        TEST_AGG_4("COUNT", "count", false)
    }
    {
        // ========
        // | count|
        // --------
        // | 10   |
        // --------
        DataSet expected;
        expected.colNames = {"count"};
        Row row;
        row.emplace_back(10);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = count(distinct col1)
        TEST_AGG_1("COUNT", "count", true)
    }
    {
        // ========
        // | count|
        // --------
        // |   0  |
        // --------
        // |   1  |
        // --------
        // |   1  |
        // --------
        // |   1  |
        // --------
        // |   1  |
        // --------
        // |   1  |
        // -------
        DataSet expected;
        expected.colNames = {"count"};
        {
            Row row;
            row.values.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(1);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = count(distinct col2)
        TEST_AGG_2("COUNT", "count", true)
    }
    {
        // ================
        // | col2 | count |
        // ----------------
        // |  0   |   1   |
        // ----------------
        // |  1   |   1   |
        // ----------------
        // |  2   |   1   |
        // ----------------
        // |  3   |   1   |
        // ----------------
        // |  4   |   1   |
        // ----------------
        // | NULL |   0   |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "count"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(1);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, count(distinct col3)
        TEST_AGG_3("COUNT", "count", true)
    }
    {
        // ========
        // | count|
        // --------
        // | 1    |
        DataSet expected;
        expected.colNames = {"count"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = count(distinct 1)
        TEST_AGG_4("COUNT", "count", true)
    }
}

TEST_F(AggregateTest, Sum) {
    {
        // ========
        // | sum  |
        // --------
        // | 45   |
        // --------
        DataSet expected;
        expected.colNames = {"sum"};
        Row row;
        row.emplace_back(45);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = sum(col1)
        TEST_AGG_1("SUM", "sum", false)
    }
    {
        // ========
        // | sum  |
        // --------
        // |   0  |
        // --------
        // |   2  |
        // --------
        // |   4  |
        // --------
        // |   6  |
        // --------
        // |   8  |
        // --------
        // | NULL |
        // --------
        DataSet expected;
        expected.colNames = {"sum"};
        Row rowNull;
        rowNull.values.emplace_back(0);
        expected.rows.emplace_back(std::move(rowNull));
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(2 * i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = sum(col2)
        TEST_AGG_2("SUM", "sum", false)
    }
    {
        // ================
        // | col2 | sum   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   2   |
        // ----------------
        // |  3   |   2   |
        // ----------------
        // |  4   |   4   |
        // ----------------
        // | NULL | NULL  |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "sum"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back((i / 2) * 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, sum(col3)
        TEST_AGG_3("SUM", "sum", false)
    }
    {
        // ========
        // | sum  |
        // --------
        // | 11   |
        // --------
        DataSet expected;
        expected.colNames = {"sum"};
        Row row;
        row.emplace_back(11);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = sum(1)
        TEST_AGG_4("SUM", "sum", false)
    }
    {
        // ========
        // | sum  |
        // --------
        // | 45   |
        // --------
        DataSet expected;
        expected.colNames = {"sum"};
        Row row;
        row.emplace_back(45);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = sum(distinct col1)
        TEST_AGG_1("SUM", "sum", true)
    }
    {
        // ========
        // | sum  |
        // --------
        // |   0  |
        // --------
        // |   1  |
        // --------
        // |   2  |
        // --------
        // |   3  |
        // --------
        // |   4  |
        // --------
        // | NULL |
        // --------
        DataSet expected;
        expected.colNames = {"sum"};
        Row rowNull;
        rowNull.values.emplace_back(0);
        expected.rows.emplace_back(std::move(rowNull));
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }

        // key = col2
        // items = sum(distinct col2)
        TEST_AGG_2("SUM", "sum", true)
    }
    {
        // ================
        // | col2 | sum   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   1   |
        // ----------------
        // |  3   |   1   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        // | NULL | NULL  |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "sum"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back((i / 2));
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, sum(distinct col3)
        TEST_AGG_3("SUM", "sum", true)
    }
    {
        // ========
        // | sum  |
        // --------
        // | 1    |
        // --------
        DataSet expected;
        expected.colNames = {"sum"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = sum(distinct 1)
        TEST_AGG_4("SUM", "sum", true)
    }
}

TEST_F(AggregateTest, Avg) {
    {
        // ========
        // | avg  |
        // --------
        // | 4.5  |
        // --------
        DataSet expected;
        expected.colNames = {"avg"};
        Row row;
        row.emplace_back(4.5);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = avg(col1)
        TEST_AGG_1("AVG", "avg", false)
    }
    {
        // ========
        // | avg  |
        // --------
        // |   0  |
        // --------
        // |   1  |
        // --------
        // |   2  |
        // --------
        // |   3  |
        // --------
        // |   4  |
        // --------
        // | NULL |
        // --------
        DataSet expected;
        expected.colNames = {"avg"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2
        // items = avg(col2)
        TEST_AGG_2("AVG", "avg", false)
    }
    {
        // ================
        // | col2 | avg   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   1   |
        // ----------------
        // |  3   |   1   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        // | NULL | NULL  |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "avg"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, sum(col3)
        TEST_AGG_3("AVG", "avg", false)
    }
    {
        // ========
        // | avg  |
        // --------
        // | 1    |
        // --------
        DataSet expected;
        expected.colNames = {"avg"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = avg(1)
        TEST_AGG_4("AVG", "avg", false)
    }
    {
        // ========
        // | avg  |
        // --------
        // | 4.5  |
        // --------
        DataSet expected;
        expected.colNames = {"avg"};
        Row row;
        row.emplace_back(4.5);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = avg(col1)
        TEST_AGG_1("AVG", "avg", true)
    }
    {
        // ========
        // | avg  |
        // --------
        // |   0  |
        // --------
        // |   1  |
        // --------
        // |   2  |
        // --------
        // |   3  |
        // --------
        // |   4  |
        // --------
        // | NULL |
        // --------
        DataSet expected;
        expected.colNames = {"avg"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2
        // items = avg(col2)
        TEST_AGG_2("AVG", "avg", true)
    }
    {
        // ================
        // | col2 | avg   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   1   |
        // ----------------
        // |  3   |   1   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        // | NULL | NULL  |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "avg"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, sum(col3)
        TEST_AGG_3("AVG", "avg", true)
    }
    {
        // ========
        // | avg  |
        // --------
        // | 1    |
        // --------
        DataSet expected;
        expected.colNames = {"avg"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = avg(1)
        TEST_AGG_4("AVG", "avg", true)
    }
}

TEST_F(AggregateTest, Max) {
    {
        // ========
        // | max  |
        // --------
        // |  9   |
        // --------
        DataSet expected;
        expected.colNames = {"max"};
        Row row;
        row.emplace_back(9);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = max(col1)
        TEST_AGG_1("MAX", "max", false)
    }
    {
        // ================
        // | col2 | max   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   1   |
        // ----------------
        // |  3   |   1   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        // | NULL | NULL  |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "max"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, max(col3)
        TEST_AGG_3("MAX", "max", false)
    }
    {
        // ========
        // | max  |
        // --------
        // | 1    |
        // --------
        DataSet expected;
        expected.colNames = {"max"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = max(1)
        TEST_AGG_4("MAX", "max", false)
    }
    {
        // ========
        // | max  |
        // --------
        // |  9   |
        // --------
        DataSet expected;
        expected.colNames = {"max"};
        Row row;
        row.emplace_back(9);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = max(col1)
        TEST_AGG_1("MAX", "max", true)
    }
    {
        // ================
        // | col2 | max   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   1   |
        // ----------------
        // |  3   |   1   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        // | NULL | NULL  |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "max"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, max(col3)
        TEST_AGG_3("MAX", "max", true)
    }
    {
        // ========
        // | max  |
        // --------
        // | 1    |
        // --------
        DataSet expected;
        expected.colNames = {"max"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = max(1)
        TEST_AGG_4("MAX", "max", true)
    }
}

TEST_F(AggregateTest, Min) {
    {
        // ========
        // | min  |
        // --------
        // | 1    |
        // --------
        DataSet expected;
        expected.colNames = {"min"};
        Row row;
        row.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = min(col1)
        TEST_AGG_1("MIN", "min", false)
    }
    {
        // ================
        // | col2 | min   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   1   |
        // ----------------
        // |  3   |   1   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        // | NULL | NULL  |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "min"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, min(col3)
        TEST_AGG_3("MIN", "min", false)
    }
    {
        // ========
        // | min  |
        // --------
        // | 1    |
        // --------
        DataSet expected;
        expected.colNames = {"min"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = min(1)
        TEST_AGG_4("MIN", "min", false)
    }
    {
        // ========
        // | min  |
        // --------
        // | 1    |
        // --------
        DataSet expected;
        expected.colNames = {"min"};
        Row row;
        row.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = min(distinct col1)
        TEST_AGG_1("MIN", "min", true)
    }
    {
        // ================
        // | col2 | min   |
        // ----------------
        // |  0   |   0   |
        // ----------------
        // |  1   |   0   |
        // ----------------
        // |  2   |   1   |
        // ----------------
        // |  3   |   1   |
        // ----------------
        // |  4   |   2   |
        // ----------------
        // | NULL | NULL  |
        // ----------------
        DataSet expected;
        expected.colNames = {"col2", "min"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, min(distinct col3)
        TEST_AGG_3("MIN", "min", true)
    }
    {
        // ========
        // | min  |
        // --------
        // | 1    |
        // --------
        DataSet expected;
        expected.colNames = {"min"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = min(distinct 1)
        TEST_AGG_4("MIN", "min", true)
    }
}

TEST_F(AggregateTest, Stdev) {
    {
        // ===============
        // | stdev       |
        // ---------------
        // |2.87228132327|
        // ---------------
        DataSet expected;
        expected.colNames = {"stdev"};
        Row row;
        row.emplace_back(2.87228132327);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = stdev(col1)
        TEST_AGG_1("STD", "stdev", false)
    }
    {
        // ===============
        // | stdev       |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   NULL      |
        DataSet expected;
        expected.colNames = {"stdev"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2
        // items = stdev(col2)
        TEST_AGG_2("STD", "stdev", false)
    }
    {
        // =======================
        // | col2 | stdev       |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   0          |
        // -----------------------
        // |  3   |   0          |
        // -----------------------
        // |  4   |   0          |
        // -----------------------
        // | NULL | NULL         |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "stdev"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, stdev(col3)
        TEST_AGG_3("STD", "stdev", false)
    }
    {
        // ===============
        // | stdev       |
        // ---------------
        // |  0          |
        DataSet expected;
        expected.colNames = {"stdev"};
        Row row;
        row.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = stdev(1)
        TEST_AGG_4("STD", "stdev", false)
    }
    {
        // ===============
        // | stdev       |
        // ---------------
        // |2.87228132327|
        // ---------------
        DataSet expected;
        expected.colNames = {"stdev"};
        Row row;
        row.emplace_back(2.87228132327);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = stdev(distinct col1)
        TEST_AGG_1("STD", "stdev", true)
    }
    {
        // ===============
        // | stdev       |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   NULL      |
        DataSet expected;
        expected.colNames = {"stdev"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2
        // items = stdev(distinct col2)
        TEST_AGG_2("STD", "stdev", true)
    }
    {
        // =======================
        // | col2 | stdev       |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   0          |
        // -----------------------
        // |  3   |   0          |
        // -----------------------
        // |  4   |   0          |
        // -----------------------
        // | NULL | NULL         |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "stdev"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, stdev(distinct col3)
        TEST_AGG_3("STD", "stdev", true)
    }
    {
        // ===============
        // | stdev       |
        // ---------------
        // |  0          |
        // ---------------
        DataSet expected;
        expected.colNames = {"stdev"};
        Row row;
        row.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = stdev(distinct 1)
        TEST_AGG_4("STD", "stdev", true)
    }
}

TEST_F(AggregateTest, BitAnd) {
    {
        // ===============
        // | bit_and     |
        // ---------------
        // |     0       |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_and"};
        Row row;
        row.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_and(col1)
        TEST_AGG_1("BIT_AND", "bit_and", false)
    }
    {
        // ===============
        // | bit_and     |
        // ---------------
        // |   0         |
        // ---------------
        // |   1         |
        // ---------------
        // |   2         |
        // ---------------
        // |   3         |
        // ---------------
        // |   4         |
        // ---------------
        // |   NULL      |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_and"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2
        // items = bit_and(col2)
        TEST_AGG_2("BIT_AND", "bit_and", false)
    }
    {
        // =======================
        // | col2 | bit_and      |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   1          |
        // -----------------------
        // |  3   |   1          |
        // -----------------------
        // |  4   |   2          |
        // -----------------------
        // | NULL | NULL         |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "bit_and"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, bit_and(col3)
        TEST_AGG_3("BIT_AND", "bit_and", false)
    }
    {
        // ===============
        // | bit_and     |
        // ---------------
        // |     1       |
        DataSet expected;
        expected.colNames = {"bit_and"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_and(1)
        TEST_AGG_4("BIT_AND", "bit_and", false)
    }
    {
        // ===============
        // | bit_and     |
        // ---------------
        // |     0       |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_and"};
        Row row;
        row.emplace_back(0);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_and(distinct col1)
        TEST_AGG_1("BIT_AND", "bit_and", true)
    }
    {
        // ===============
        // | bit_and     |
        // ---------------
        // |   0         |
        // ---------------
        // |   1         |
        // ---------------
        // |   2         |
        // ---------------
        // |   3         |
        // ---------------
        // |   4         |
        // ---------------
        // |   NULL      |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_and"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2
        // items = bit_and(col2)
        TEST_AGG_2("BIT_AND", "bit_and", true)
    }
    {
        // =======================
        // | col2 | bit_and      |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   1          |
        // -----------------------
        // |  3   |   1          |
        // -----------------------
        // |  4   |   2          |
        // -----------------------
        // | NULL | NULL         |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "bit_and"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, bit_and(col3)
        TEST_AGG_3("BIT_AND", "bit_and", true)
    }
    {
        // ===============
        // | bit_and     |
        // ---------------
        // |     1       |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_and"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_and(1)
        TEST_AGG_4("BIT_AND", "bit_and", true)
    }
}

TEST_F(AggregateTest, BitOr) {
    {
        // ===============
        // | bit_or      |
        // ---------------
        // |    15       |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_or"};
        Row row;
        row.emplace_back(15);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_or(col1)
        TEST_AGG_1("BIT_OR", "bit_or", false)
    }
    {
        // ===============
        // | bit_or      |
        // ---------------
        // |   0         |
        // ---------------
        // |   1         |
        // ---------------
        // |   2         |
        // ---------------
        // |   3         |
        // ---------------
        // |   4         |
        // ---------------
        // |   NULL      |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_or"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2
        // items = bit_or(col2)
        TEST_AGG_2("BIT_OR", "bit_or", false)
    }
    {
        // =======================
        // | col2 | bit_or       |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   1          |
        // -----------------------
        // |  3   |   1          |
        // -----------------------
        // |  4   |   2          |
        // -----------------------
        // | NULL | NULL         |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "bit_or"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, bit_or(col3)
        TEST_AGG_3("BIT_OR", "bit_or", false)
    }
    {
        // ===============
        // | bit_or      |
        // ---------------
        // |    1        |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_or"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_or(1)
        TEST_AGG_4("BIT_OR", "bit_or", false)
    }
    {
        // ===============
        // | bit_or      |
        // ---------------
        // |    15       |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_or"};
        Row row;
        row.emplace_back(15);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_or(distinct col1)
        TEST_AGG_1("BIT_OR", "bit_or", true)
    }
    {
        // ===============
        // | bit_or      |
        // ---------------
        // |   0         |
        // ---------------
        // |   1         |
        // ---------------
        // |   2         |
        // ---------------
        // |   3         |
        // ---------------
        // |   4         |
        // ---------------
        // |   NULL      |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_or"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2
        // items = bit_or(distinct col2)
        TEST_AGG_2("BIT_OR", "bit_or", true)
    }
    {
        // =======================
        // | col2 | bit_or       |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   1          |
        // -----------------------
        // |  3   |   1          |
        // -----------------------
        // |  4   |   2          |
        // -----------------------
        // | NULL | NULL         |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "bit_or"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, bit_or(distinct col3)
        TEST_AGG_3("BIT_OR", "bit_or", true)
    }
    {
        // ===============
        // | bit_or      |
        // ---------------
        // |    1        |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_or"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_or(distinct 1)
        TEST_AGG_4("BIT_OR", "bit_or", true)
    }
}

TEST_F(AggregateTest, BitXor) {
    {
        // ===============
        // | bit_xor     |
        // ---------------
        // |    1        |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_xor"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_xor(col1)
        TEST_AGG_1("BIT_XOR", "bit_xor", false)
    }
    {
        // ===============
        // | bit_xor     |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   0         |
        // ---------------
        // |   NULL      |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_xor"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2
        // items = bit_xor(col2)
        TEST_AGG_2("BIT_XOR", "bit_xor", false)
    }
    {
        // =======================
        // | col2 | bit_xor      |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   0          |
        // -----------------------
        // |  3   |   0          |
        // -----------------------
        // |  4   |   0          |
        // -----------------------
        // | NULL | NULL         |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "bit_xor"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(0);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, bit_xor(col3)
        TEST_AGG_3("BIT_XOR", "bit_xor", false)
    }
    {
        // ===============
        // | bit_xor     |
        // ---------------
        // |    1        |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_xor"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_xor(1)
        TEST_AGG_4("BIT_XOR", "bit_xor", false)
    }
    {
        // ===============
        // | bit_xor     |
        // ---------------
        // |    1        |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_xor"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_xor(distinct col1)
        TEST_AGG_1("BIT_XOR", "bit_xor", true)
    }
    {
        // ===============
        // | bit_xor     |
        // ---------------
        // |   0         |
        // ---------------
        // |   1         |
        // ---------------
        // |   2         |
        // ---------------
        // |   3         |
        // ---------------
        // |   4         |
        // ---------------
        // |   NULL      |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_xor"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2
        // items = bit_xor(distinct col2)
        TEST_AGG_2("BIT_XOR", "bit_xor", true)
    }
    {
        // =======================
        // | col2 | bit_xor      |
        // -----------------------
        // |  0   |   0          |
        // -----------------------
        // |  1   |   0          |
        // -----------------------
        // |  2   |   1          |
        // -----------------------
        // |  3   |   1          |
        // -----------------------
        // |  4   |   2          |
        // -----------------------
        // | NULL | NULL         |
        // -----------------------
        DataSet expected;
        expected.colNames = {"col2", "bit_xor"};
        for (auto i = 0; i < 5; ++i) {
            Row row;
            row.values.emplace_back(i);
            row.values.emplace_back(i / 2);
            expected.rows.emplace_back(std::move(row));
        }
        Row row;
        row.values.emplace_back(Value::kNullValue);
        row.values.emplace_back(Value::kNullValue);
        expected.rows.emplace_back(std::move(row));

        // key = col2, col3
        // items = col2, bit_xor(distinct col3)
        TEST_AGG_3("BIT_XOR", "bit_xor", true)
    }
    {
        // ===============
        // | bit_xor     |
        // ---------------
        // |    1        |
        // ---------------
        DataSet expected;
        expected.colNames = {"bit_xor"};
        Row row;
        row.emplace_back(1);
        expected.rows.emplace_back(std::move(row));

        // key =
        // items = bit_xor(distinct 1)
        TEST_AGG_4("BIT_XOR", "bit_xor", true)
    }
}
}   // namespace graph
}   // namespace nebula
