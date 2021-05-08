/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "executor/query/UnionExecutor.h"

#include <memory>

#include <folly/String.h>
#include <gtest/gtest.h>

#include "context/ExecutionContext.h"
#include "context/QueryContext.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"

using folly::stringPrintf;

namespace nebula {
namespace graph {

class SetExecutorTest : public ::testing::Test {
public:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
    }

    static bool diffDataSet(const DataSet& lhs, const DataSet& rhs) {
        if (lhs.colNames != rhs.colNames) return false;
        if (lhs.rows.size() != rhs.rows.size()) return false;

        auto comp = [](const Row& l, const Row& r) -> bool {
            for (size_t i = 0; i < l.values.size(); ++i) {
                if (!(l.values[i] < r.values[i])) return false;
            }
            return true;
        };

        // Following sort will change the input data sets, so make the copies
        auto l = lhs;
        auto r = rhs;
        std::sort(l.rows.begin(), l.rows.end(), comp);
        std::sort(r.rows.begin(), r.rows.end(), comp);
        return l.rows == r.rows;
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
};

TEST_F(SetExecutorTest, TestUnionAll) {
    auto testUnion = [this](const DataSet& lds, const DataSet& rds, const DataSet& expected) {
        auto left = StartNode::make(qctx_.get());
        auto right = StartNode::make(qctx_.get());
        auto unionNode = Union::make(qctx_.get(), left, right);
        unionNode->setLeftVar(left->outputVar());
        unionNode->setRightVar(right->outputVar());

        auto unionExecutor = Executor::create(unionNode, qctx_.get());
        ResultBuilder lb, rb;
        lb.value(Value(lds)).iter(Iterator::Kind::kSequential);
        rb.value(Value(rds)).iter(Iterator::Kind::kSequential);
        // Must save the values after constructing executors
        qctx_->ectx()->setResult(left->outputVar(), lb.finish());
        qctx_->ectx()->setResult(right->outputVar(), rb.finish());
        auto future = unionExecutor->execute();
        EXPECT_TRUE(std::move(future).get().ok());

        auto& result = qctx_->ectx()->getResult(unionNode->outputVar());
        EXPECT_TRUE(result.value().isDataSet());

        DataSet resultDS = result.value().getDataSet();
        EXPECT_TRUE(diffDataSet(resultDS, expected)) << "\nResult dataset: \n"
                                                     << resultDS << "Expected dataset: \n"
                                                     << expected;
    };

    std::vector<std::string> colNames = {"col1", "col2"};
    // Left and right are not empty
    {
        DataSet lds;
        lds.colNames = colNames;
        lds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(2), Value("row2")}),
        };

        DataSet rds;
        rds.colNames = colNames;
        rds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(3), Value("row3")}),
        };

        DataSet expected;
        expected.colNames = colNames;
        expected.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(1), Value("row1")}),
            Row({Value(2), Value("row2")}),
            Row({Value(3), Value("row3")}),
        };

        testUnion(lds, rds, expected);
    }
    // Left is empty
    {
        DataSet lds;
        lds.colNames = colNames;

        DataSet rds;
        rds.colNames = colNames;
        rds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(3), Value("row3")}),
        };
        DataSet expected;
        expected.colNames = colNames;
        expected.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(3), Value("row3")}),
        };

        testUnion(lds, rds, expected);
    }
    // Right is empty
    {
        DataSet lds;
        lds.colNames = colNames;
        lds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(2), Value("row2")}),
        };

        DataSet rds;
        rds.colNames = colNames;

        DataSet expected;
        expected.colNames = colNames;
        expected.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(2), Value("row2")}),
        };

        testUnion(lds, rds, expected);
    }
    // All empty
    {
        DataSet lds;
        lds.colNames = colNames;

        DataSet rds;
        rds.colNames = colNames;

        DataSet expected;
        expected.colNames = colNames;

        testUnion(lds, rds, expected);
    }
}

TEST_F(SetExecutorTest, TestGetNeighobrsIterator) {
    auto left = StartNode::make(qctx_.get());
    auto right = StartNode::make(qctx_.get());
    auto unionNode = Union::make(qctx_.get(), left, right);
    unionNode->setLeftVar(left->outputVar());
    unionNode->setRightVar(right->outputVar());

    auto unionExecutor = Executor::create(unionNode, qctx_.get());

    DataSet lds;
    lds.colNames = {"col1"};
    DataSet rds;
    rds.colNames = {"col1", "col2"};

    // Must save the values after constructing executors
    ResultBuilder lrb, rrb;
    auto& lRes = lrb.value(Value(std::move(lds))).iter(Iterator::Kind::kGetNeighbors);
    auto& rRes = rrb.value(Value(std::move(rds)));
    qctx_->ectx()->setResult(left->outputVar(), lRes.finish());
    qctx_->ectx()->setResult(right->outputVar(), rRes.finish());
    auto future = unionExecutor->execute();
    auto status = std::move(future).get();

    EXPECT_FALSE(status.ok());

    auto expected = "Invalid iterator kind: get neighbors iterator vs. sequential iterator";
    EXPECT_EQ(status.toString(), expected);
}

TEST_F(SetExecutorTest, TestUnionDifferentColumns) {
    auto left = StartNode::make(qctx_.get());
    auto right = StartNode::make(qctx_.get());
    auto unionNode = Union::make(qctx_.get(), left, right);
    unionNode->setLeftVar(left->outputVar());
    unionNode->setRightVar(right->outputVar());

    auto unionExecutor = Executor::create(unionNode, qctx_.get());

    DataSet lds;
    lds.colNames = {"col1"};
    DataSet rds;
    rds.colNames = {"col1", "col2"};

    // Must save the values after constructing executors
    qctx_->ectx()->setValue(left->outputVar(), Value(lds));
    qctx_->ectx()->setValue(right->outputVar(), Value(rds));
    auto future = unionExecutor->execute();
    auto status = std::move(future).get();

    EXPECT_FALSE(status.ok());

    auto expected = "Datasets have different columns: <col1> vs. <col1,col2>";
    EXPECT_EQ(status.toString(), expected);
}

TEST_F(SetExecutorTest, TestUnionDifferentValueType) {
    auto left = StartNode::make(qctx_.get());
    auto right = StartNode::make(qctx_.get());
    auto unionNode = Union::make(qctx_.get(), left, right);
    unionNode->setLeftVar(left->outputVar());
    unionNode->setRightVar(right->outputVar());

    auto unionExecutor = Executor::create(unionNode, qctx_.get());

    List lst;
    DataSet rds;

    // Must save the values after constructing executors
    qctx_->ectx()->setValue(left->outputVar(), Value(lst));
    qctx_->ectx()->setValue(right->outputVar(), Value(rds));
    auto future = unionExecutor->execute();
    auto status = std::move(future).get();

    EXPECT_FALSE(status.ok());

    std::stringstream ss;
    ss << "Invalid data types of dependencies: " << Value::Type::LIST << " vs. "
       << Value::Type::DATASET << ".";
    EXPECT_EQ(status.toString(), ss.str());
}

TEST_F(SetExecutorTest, TestIntersect) {
    auto testInterset = [this](const DataSet& lds, const DataSet& rds, const DataSet& expected) {
        auto left = StartNode::make(qctx_.get());
        auto right = StartNode::make(qctx_.get());
        auto intersect = Intersect::make(qctx_.get(), left, right);
        intersect->setLeftVar(left->outputVar());
        intersect->setRightVar(right->outputVar());

        ResultBuilder lb, rb;
        lb.value(Value(lds)).iter(Iterator::Kind::kSequential);
        rb.value(Value(rds)).iter(Iterator::Kind::kSequential);
        auto executor = Executor::create(intersect, qctx_.get());
        qctx_->ectx()->setResult(left->outputVar(), lb.finish());
        qctx_->ectx()->setResult(right->outputVar(), rb.finish());

        auto fut = executor->execute();
        auto status = std::move(fut).get();
        EXPECT_TRUE(status.ok());

        auto& result = qctx_->ectx()->getResult(intersect->outputVar());
        EXPECT_TRUE(result.value().isDataSet());

        DataSet ds;
        ds.colNames = lds.colNames;
        for (auto iter = result.iter(); iter->valid(); iter->next()) {
            Row row;
            for (auto& col : ds.colNames) {
                row.values.emplace_back(iter->getColumn(col));
            }
            ds.emplace_back(std::move(row));
        }
        EXPECT_TRUE(diffDataSet(ds, expected));
    };
    // Right and left are not empty
    {
        DataSet lds;
        lds.colNames = {"col1", "col2"};
        lds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(1), Value("row1")}),
            Row({Value(2), Value("row2")}),
        };

        DataSet rds;
        rds.colNames = {"col1", "col2"};
        rds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(3), Value("row3")}),
        };
        DataSet expected;
        expected.colNames = {"col1", "col2"};
        expected.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(1), Value("row1")}),
        };
        testInterset(lds, rds, expected);
    }
    // Right is empty
    {
        DataSet lds;
        lds.colNames = {"col1", "col2"};
        lds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(1), Value("row1")}),
            Row({Value(2), Value("row2")}),
        };

        DataSet rds;
        rds.colNames = {"col1", "col2"};

        DataSet expected;
        expected.colNames = {"col1", "col2"};

        testInterset(lds, rds, expected);
    }
    // Left is empty
    {
        DataSet lds;
        lds.colNames = {"col1", "col2"};

        DataSet rds;
        rds.colNames = {"col1", "col2"};
        rds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(3), Value("row3")}),
        };

        DataSet expected;
        expected.colNames = {"col1", "col2"};

        testInterset(lds, rds, expected);
    }
    // All empty
    {
        DataSet lds;
        lds.colNames = {"col1", "col2"};

        DataSet rds;
        rds.colNames = {"col1", "col2"};

        DataSet expected;
        expected.colNames = {"col1", "col2"};

        testInterset(lds, rds, expected);
    }
}

TEST_F(SetExecutorTest, TestMinus) {
    auto testMinus = [this](const DataSet& lds, const DataSet& rds, const DataSet& expected) {
        auto left = StartNode::make(qctx_.get());
        auto right = StartNode::make(qctx_.get());
        auto minus = Minus::make(qctx_.get(), left, right);
        minus->setLeftVar(left->outputVar());
        minus->setRightVar(right->outputVar());

        ResultBuilder lb, rb;
        lb.value(Value(lds)).iter(Iterator::Kind::kSequential);
        rb.value(Value(rds)).iter(Iterator::Kind::kSequential);
        auto executor = Executor::create(minus, qctx_.get());
        qctx_->ectx()->setResult(left->outputVar(), lb.finish());
        qctx_->ectx()->setResult(right->outputVar(), rb.finish());

        auto fut = executor->execute();
        auto status = std::move(fut).get();
        EXPECT_TRUE(status.ok());

        auto& result = qctx_->ectx()->getResult(minus->outputVar());
        EXPECT_TRUE(result.value().isDataSet());

        DataSet ds;
        ds.colNames = lds.colNames;
        for (auto iter = result.iter(); iter->valid(); iter->next()) {
            Row row;
            for (auto& col : ds.colNames) {
                row.values.emplace_back(iter->getColumn(col));
            }
            ds.emplace_back(std::move(row));
        }
        EXPECT_TRUE(diffDataSet(ds, expected));
    };
    // Left and right are not empty
    {
        DataSet lds;
        lds.colNames = {"col1", "col2"};
        lds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(1), Value("row1")}),
            Row({Value(2), Value("row2")}),
        };

        DataSet rds;
        rds.colNames = {"col1", "col2"};
        rds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(3), Value("row3")}),
        };

        DataSet expected;
        expected.colNames = {"col1", "col2"};
        expected.rows = {
            Row({Value(2), Value("row2")}),
        };

        testMinus(lds, rds, expected);
    }
    // Left is empty
    {
        DataSet lds;
        lds.colNames = {"col1", "col2"};

        DataSet rds;
        rds.colNames = {"col1", "col2"};
        rds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(3), Value("row3")}),
        };

        DataSet expected;
        expected.colNames = {"col1", "col2"};

        testMinus(lds, rds, expected);
    }
    // Right is empty
    {
        DataSet lds;
        lds.colNames = {"col1", "col2"};
        lds.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(1), Value("row1")}),
            Row({Value(2), Value("row2")}),
        };

        DataSet rds;
        rds.colNames = {"col1", "col2"};

        DataSet expected;
        expected.colNames = {"col1", "col2"};
        expected.rows = {
            Row({Value(1), Value("row1")}),
            Row({Value(1), Value("row1")}),
            Row({Value(2), Value("row2")}),
        };

        testMinus(lds, rds, expected);
    }
    // All empty
    {
        DataSet lds;
        lds.colNames = {"col1", "col2"};

        DataSet rds;
        rds.colNames = {"col1", "col2"};

        DataSet expected;
        expected.colNames = {"col1", "col2"};

        testMinus(lds, rds, expected);
    }
}

}   // namespace graph
}   // namespace nebula
