/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/plan/Algo.h"
#include "executor/algo/CartesianProductExecutor.h"

namespace nebula {
namespace graph {
class CartesianProductTest : public testing::Test {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        {
            DataSet ds1;
            ds1.colNames = {kSrc};
            for (auto i = 0; i < 5; ++i) {
                Row row;
                row.values.emplace_back(folly::to<std::string>(i));
                ds1.rows.emplace_back(std::move(row));
            }
            auto* var1 = qctx_->symTable()->newVariable("ds1");
            var1->colNames = ds1.colNames;
            qctx_->ectx()->setResult("ds1",
                                     ResultBuilder()
                                         .value(Value(std::move(ds1)))
                                         .iter(Iterator::Kind::kSequential)
                                         .finish());

            DataSet ds2;
            ds2.colNames = {kDst};
            for (auto i = 0; i < 3; ++i) {
                Row row;
                row.values.emplace_back(folly::to<std::string>(static_cast<char>(i + 'a')));
                ds2.rows.emplace_back(std::move(row));
            }
            auto* var2 = qctx_->symTable()->newVariable("ds2");
            var2->colNames = ds2.colNames;
            qctx_->ectx()->setResult("ds2",
                                     ResultBuilder()
                                         .value(Value(std::move(ds2)))
                                         .iter(Iterator::Kind::kSequential)
                                         .finish());

            DataSet ds3;
            ds3.colNames = {"col1", "col2"};
            for (auto i = 0; i < 3; ++i) {
                Row row;
                row.values.emplace_back(folly::to<std::string>(static_cast<char>(i + 'x')));
                row.values.emplace_back(folly::to<std::string>(static_cast<char>(i + 'X')));
                ds3.rows.emplace_back(std::move(row));
            }
            auto* var3 = qctx_->symTable()->newVariable("ds3");
            var3->colNames = ds3.colNames;
            qctx_->ectx()->setResult("ds3",
                                     ResultBuilder()
                                         .value(Value(std::move(ds3)))
                                         .iter(Iterator::Kind::kSequential)
                                         .finish());
        }
    }

    void checkResult(DataSet& expected, const std::string& output) {
        auto& result = qctx_->ectx()->getResult(output);
        DataSet resultDs;
        resultDs.colNames = expected.colNames;
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

        EXPECT_EQ(resultDs, expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
};

TEST_F(CartesianProductTest, twoVars) {
    auto* cp = CartesianProduct::make(qctx_.get(), nullptr);
    std::vector<std::string> colNames = {kSrc, kDst};
    cp->addVar("ds1");
    cp->addVar("ds2");
    cp->setColNames(colNames);

    auto cpExe = std::make_unique<CartesianProductExecutor>(cp, qctx_.get());
    auto future = cpExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());

    DataSet expected;
    expected.colNames = colNames;
    for (size_t i = 0; i < 5; ++i) {
        for (size_t j = 0; j < 3; ++j) {
            Row row;
            row.values.emplace_back(folly::to<std::string>(i));
            row.values.emplace_back(folly::to<std::string>(static_cast<char>(j + 'a')));
            expected.rows.emplace_back(std::move(row));
        }
    }
    checkResult(expected, cp->outputVar());
}

TEST_F(CartesianProductTest, thressVars) {
    auto* cp = CartesianProduct::make(qctx_.get(), nullptr);
    cp->addVar("ds1");
    cp->addVar("ds2");
    cp->addVar("ds3");
    std::vector<std::string> colNames = {kSrc, kDst, "col1", "col2"};
    cp->setColNames(colNames);

    auto cpExe = std::make_unique<CartesianProductExecutor>(cp, qctx_.get());
    auto future = cpExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());

    DataSet expected;
    expected.colNames = colNames;
    for (size_t i = 0; i < 5; ++i) {
        for (size_t j = 0; j < 3; ++j) {
            for (size_t k = 0; k < 3; ++k) {
                Row row;
                row.values.emplace_back(folly::to<std::string>(i));
                row.values.emplace_back(folly::to<std::string>(static_cast<char>(j + 'a')));
                row.values.emplace_back(folly::to<std::string>(static_cast<char>(k + 'x')));
                row.values.emplace_back(folly::to<std::string>(static_cast<char>(k + 'X')));
                expected.rows.emplace_back(std::move(row));
            }
        }
    }
    checkResult(expected, cp->outputVar());
}

TEST_F(CartesianProductTest, otherTwoVar) {
    auto* cp = CartesianProduct::make(qctx_.get(), nullptr);
    cp->addVar("ds1");
    cp->addVar("ds3");
    std::vector<std::string> colNames = {kSrc, "col1", "col2"};
    cp->setColNames(colNames);

    auto cpExe = std::make_unique<CartesianProductExecutor>(cp, qctx_.get());
    auto future = cpExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());

    DataSet expected;
    expected.colNames = colNames;
    for (size_t i = 0; i < 5; ++i) {
        for (size_t j = 0; j < 3; ++j) {
            Row row;
            row.values.emplace_back(folly::to<std::string>(i));
            row.values.emplace_back(folly::to<std::string>(static_cast<char>(j + 'x')));
            row.values.emplace_back(folly::to<std::string>(static_cast<char>(j + 'X')));
            expected.rows.emplace_back(std::move(row));
        }
    }
    checkResult(expected, cp->outputVar());
}

}   // namespace graph
}   // namespace nebula
