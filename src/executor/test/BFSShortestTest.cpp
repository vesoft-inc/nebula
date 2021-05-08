/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/plan/Algo.h"
#include "executor/algo/BFSShortestPathExecutor.h"

namespace nebula {
namespace graph {
class BFSShortestTest : public testing::Test {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        {
            DataSet ds1;
            ds1.colNames = {kVid,
                            "_stats",
                            "_edge:+edge1:_type:_dst:_rank",
                            "_expr"};
            {
                Row row;
                // _vid
                row.values.emplace_back("1");
                // _stats = empty
                row.values.emplace_back(Value());
                // edges
                List edges;
                for (auto j = 2; j < 4; ++j) {
                    List edge;
                    edge.values.emplace_back(1);
                    edge.values.emplace_back(folly::to<std::string>(j));
                    edge.values.emplace_back(0);
                    edges.values.emplace_back(std::move(edge));
                }
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds1.rows.emplace_back(std::move(row));
            }
            firstStepResult_ = std::move(ds1);

            DataSet ds2;
            ds2.colNames = {kVid,
                            "_stats",
                            "_edge:+edge1:_type:_dst:_rank",
                            "_expr"};
            {
                Row row;
                // _vid
                row.values.emplace_back("2");
                // _stats = empty
                row.values.emplace_back(Value());
                // edges
                List edges;
                for (auto j = 4; j < 6; ++j) {
                    List edge;
                    edge.values.emplace_back(1);
                    edge.values.emplace_back(folly::to<std::string>(j));
                    edge.values.emplace_back(0);
                    edges.values.emplace_back(std::move(edge));
                }
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds2.rows.emplace_back(std::move(row));
            }
            {
                Row row;
                // _vid
                row.values.emplace_back("3");
                // _stats = empty
                row.values.emplace_back(Value());
                // edges
                List edges;
                {
                    List edge;
                    edge.values.emplace_back(1);
                    edge.values.emplace_back("1");
                    edge.values.emplace_back(0);
                    edges.values.emplace_back(std::move(edge));
                }
                {
                    List edge;
                    edge.values.emplace_back(1);
                    edge.values.emplace_back("4");
                    edge.values.emplace_back(0);
                    edges.values.emplace_back(std::move(edge));
                }

                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds2.rows.emplace_back(std::move(row));
            }
            secondStepResult_ = std::move(ds2);
        }
        {
            DataSet ds;
            ds.colNames = {kVid,
                            "_stats",
                            "_tag:tag1:prop1:prop2",
                            "_edge:+edge1:prop1:prop2:_dst:_rank",
                            "_expr"};
            qctx_->symTable()->newVariable("empty_get_neighbors");
            qctx_->ectx()->setResult("empty_get_neighbors",
                                     ResultBuilder()
                                         .value(Value(std::move(ds)))
                                         .iter(Iterator::Kind::kGetNeighbors)
                                         .finish());
        }
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
    DataSet                       firstStepResult_;
    DataSet                       secondStepResult_;
};

TEST_F(BFSShortestTest, BFSShortest) {
    qctx_->symTable()->newVariable("input");

    auto* bfs = BFSShortestPath::make(qctx_.get(), nullptr);
    bfs->setInputVar("input");
    bfs->setColNames({"_vid", "edge"});

    auto bfsExe = std::make_unique<BFSShortestPathExecutor>(bfs, qctx_.get());
    // Step 1
    {
        ResultBuilder builder;
        List datasets;
        datasets.values.emplace_back(std::move(firstStepResult_));
        builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
        qctx_->ectx()->setResult("input", builder.finish());

        auto future = bfsExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(bfs->outputVar());

        DataSet expected;
        expected.colNames = {"_vid", "edge"};
        {
            Row row;
            row.values = {"2", Edge("1", "2", 1, "edge1", 0, {})};
            expected.rows.emplace_back(std::move(row));
        }
        {
            Row row;
            row.values = {"3", Edge("1", "3", 1, "edge1", 0, {})};
            expected.rows.emplace_back(std::move(row));
        }

        std::sort(expected.rows.begin(), expected.rows.end());
        auto resultDs = result.value().getDataSet();
        std::sort(resultDs.rows.begin(), resultDs.rows.end());
        EXPECT_EQ(resultDs, expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    // Step 2
    {
        ResultBuilder builder;
        List datasets;
        datasets.values.emplace_back(std::move(secondStepResult_));
        builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
        qctx_->ectx()->setResult("input", builder.finish());

        auto future = bfsExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(bfs->outputVar());

        DataSet expected;
        expected.colNames = {"_vid", "edge"};
        {
            Row row;
            row.values = {"4", Edge("2", "4", 1, "edge1", 0, {})};
            expected.rows.emplace_back(std::move(row));
        }
        {
            Row row;
            row.values = {"5", Edge("2", "5", 1, "edge1", 0, {})};
            expected.rows.emplace_back(std::move(row));
        }
        {
            Row row;
            row.values = {"4", Edge("3", "4", 1, "edge1", 0, {})};
            expected.rows.emplace_back(std::move(row));
        }

        std::sort(expected.rows.begin(), expected.rows.end());
        auto resultDs = result.value().getDataSet();
        std::sort(resultDs.rows.begin(), resultDs.rows.end());
        EXPECT_EQ(resultDs, expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
}

TEST_F(BFSShortestTest, EmptyInput) {
    auto* bfs = BFSShortestPath::make(qctx_.get(), nullptr);
    bfs->setInputVar("empty_get_neighbors");
    bfs->setColNames({"_vid", "edge"});

    auto bfsExe = std::make_unique<BFSShortestPathExecutor>(bfs, qctx_.get());
    auto future = bfsExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(bfs->outputVar());

    DataSet expected;
    expected.colNames = {"_vid", "edge"};
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}
}  // namespace graph
}  // namespace nebula
