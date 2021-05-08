/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/plan/Algo.h"
#include "executor/algo/ProduceAllPathsExecutor.h"

namespace nebula {
namespace graph {
class ProduceAllPathsTest : public testing::Test {
protected:
    static bool compareAllPathRow(Row& lhs, Row& rhs) {
        auto& lDst = lhs.values[0];
        auto& rDst = rhs.values[0];
        if (lDst != rDst) {
            return lDst < rDst;
        }

        auto& lEdges = lhs.values[1].getList();
        auto& rEdges = rhs.values[1].getList();
        if (lEdges != rEdges) {
            return lEdges < rEdges;
        }
        return false;
    }

    static ::testing::AssertionResult verifyAllPaths(DataSet& result, DataSet& expected) {
        std::sort(expected.rows.begin(), expected.rows.end(), compareAllPathRow);
        for (auto& row : expected.rows) {
            auto& edges = row.values[1].mutableList().values;
            std::sort(edges.begin(), edges.end());
        }
        std::sort(result.rows.begin(), result.rows.end(), compareAllPathRow);
        for (auto& row : result.rows) {
            auto& edges = row.values[1].mutableList().values;
            std::sort(edges.begin(), edges.end());
        }
        EXPECT_EQ(result, expected);
        return result == expected
                   ? ::testing::AssertionSuccess()
                   : (::testing::AssertionFailure() << result << " vs. " << expected);
    }

    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        /*
        *  0->1->5->7;
        *  1->6->7
        *  2->6->7
        *  3->4->7
        *  startVids {0, 1, 2, 3}
        */
        {
            DataSet ds1;
            ds1.colNames = {kVid, "_stats", "_edge:+edge1:_type:_dst:_rank", "_expr"};
            {
                // 0->1
                Row row;
                row.values.emplace_back("0");
                // _stats = empty
                row.values.emplace_back(Value());
                // edges
                List edges;
                List edge;
                edge.values.emplace_back(1);
                edge.values.emplace_back("1");
                edge.values.emplace_back(0);
                edges.values.emplace_back(std::move(edge));
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds1.rows.emplace_back(std::move(row));
            }
            {
                // 1->5, 1->6;
                Row row;
                row.values.emplace_back("1");
                // _stats = empty
                row.values.emplace_back(Value());
                // edges
                List edges;
                for (auto i = 5; i < 7; i++) {
                    List edge;
                    edge.values.emplace_back(1);
                    edge.values.emplace_back(folly::to<std::string>(i));
                    edge.values.emplace_back(0);
                    edges.values.emplace_back(std::move(edge));
                }
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds1.rows.emplace_back(std::move(row));
            }
            {
                // 2->6
                Row row;
                row.values.emplace_back("2");
                // _stats = empty
                row.values.emplace_back(Value());
                // edges
                List edges;
                List edge;
                edge.values.emplace_back(1);
                edge.values.emplace_back("6");
                edge.values.emplace_back(0);
                edges.values.emplace_back(std::move(edge));
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds1.rows.emplace_back(std::move(row));
            }
            {
                // 3->4
                Row row;
                row.values.emplace_back("3");
                // _stats = empty
                row.values.emplace_back(Value());
                // edges
                List edges;
                List edge;
                edge.values.emplace_back(1);
                edge.values.emplace_back("4");
                edge.values.emplace_back(0);
                edges.values.emplace_back(std::move(edge));
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds1.rows.emplace_back(std::move(row));
            }
            firstStepResult_ = std::move(ds1);

            DataSet ds2;
            ds2.colNames = {kVid, "_stats", "_edge:+edge1:_type:_dst:_rank", "_expr"};
            {
                // 1->5, 1->6;
                Row row;
                row.values.emplace_back("1");
                // _stats = empty
                row.values.emplace_back(Value());
                // edges
                List edges;
                for (auto i = 5; i < 7; i++) {
                    List edge;
                    edge.values.emplace_back(1);
                    edge.values.emplace_back(folly::to<std::string>(i));
                    edge.values.emplace_back(0);
                    edges.values.emplace_back(std::move(edge));
                }
                row.values.emplace_back(edges);
                // _expr = empty
                row.values.emplace_back(Value());
                ds2.rows.emplace_back(std::move(row));
            }
            {
                // 4->7, 5->7, 6->7
                for (auto i = 4; i < 7; i++) {
                    Row row;
                    row.values.emplace_back(folly::to<std::string>(i));
                    // _stats = empty
                    row.values.emplace_back(Value());
                    // edges
                    List edges;
                    List edge;
                    edge.values.emplace_back(1);
                    edge.values.emplace_back("7");
                    edge.values.emplace_back(0);
                    edges.values.emplace_back(std::move(edge));
                    row.values.emplace_back(edges);
                    // _expr = empty
                    row.values.emplace_back(Value());
                    ds2.rows.emplace_back(std::move(row));
                }
            }
            secondStepResult_ = std::move(ds2);

            DataSet ds3;
            ds3.colNames = {kVid, "_stats", "_edge:+edge1:_type:_dst:_rank", "_expr"};
            {
                // 5->7, 6->7
                for (auto i = 5; i < 7; i++) {
                    Row row;
                    row.values.emplace_back(folly::to<std::string>(i));
                    // _stats = empty
                    row.values.emplace_back(Value());
                    // edges
                    List edges;
                    List edge;
                    edge.values.emplace_back(1);
                    edge.values.emplace_back("7");
                    edge.values.emplace_back(0);
                    edges.values.emplace_back(std::move(edge));
                    row.values.emplace_back(edges);
                    // _expr = empty
                    row.values.emplace_back(Value());
                    ds3.rows.emplace_back(std::move(row));
                }
            }
            thridStepResult_ = std::move(ds3);

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
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
    DataSet firstStepResult_;
    DataSet secondStepResult_;
    DataSet thridStepResult_;
};

TEST_F(ProduceAllPathsTest, AllPath) {
    qctx_->symTable()->newVariable("input");

    auto* allPathsNode = ProduceAllPaths::make(qctx_.get(), nullptr);
    allPathsNode->setInputVar("input");
    allPathsNode->setColNames({kDst, "_paths"});

    auto allPathsExe = std::make_unique<ProduceAllPathsExecutor>(allPathsNode, qctx_.get());

    // Step1
    {
        ResultBuilder builder;
        List datasets;
        datasets.values.emplace_back(std::move(firstStepResult_));
        builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
        qctx_->ectx()->setResult("input", builder.finish());

        auto future = allPathsExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(allPathsNode->outputVar());

        DataSet expected;
        expected.colNames = {kDst, "_paths"};
        {
            // 0->1
            Row row;
            Path path;
            path.src = Vertex("0", {});
            path.steps.emplace_back(Step(Vertex("1", {}), 1, "edge1", 0, {}));

            List paths;
            paths.values.emplace_back(std::move(path));
            row.values.emplace_back("1");
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }
        {
            // 1->5
            Row row;
            Path path;
            path.src = Vertex("1", {});
            path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));

            List paths;
            paths.values.emplace_back(std::move(path));
            row.values.emplace_back("5");
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }
        {
            // 1->6
            Row row;
            List paths;

            {
                Path path;
                path.src = Vertex("1", {});
                path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
            }
            {
                Path path;
                path.src = Vertex("2", {});
                path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
            }

            row.values.emplace_back("6");
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }
        {
            // 3->4
            Row row;
            Path path;
            path.src = Vertex("3", {});
            path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 0, {}));

            List paths;
            paths.values.emplace_back(std::move(path));
            row.values.emplace_back("4");
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }

        auto resultDs = result.value().getDataSet();
        EXPECT_TRUE(verifyAllPaths(resultDs, expected));
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
    // Step 2
    {
        ResultBuilder builder;
        List datasets;
        datasets.values.emplace_back(std::move(secondStepResult_));
        builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
        qctx_->ectx()->setResult("input", builder.finish());

        auto future = allPathsExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(allPathsNode->outputVar());

        DataSet expected;
        expected.colNames = {kDst, "_paths"};
        {
            // 0->1->5
            Row row;
            Path path;
            path.src = Vertex("0", {});
            path.steps.emplace_back(Step(Vertex("1", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));

            List paths;
            paths.values.emplace_back(std::move(path));
            row.values.emplace_back("5");
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }
        {
            // 0->1->6
            Row row;
            Path path;
            path.src = Vertex("0", {});
            path.steps.emplace_back(Step(Vertex("1", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));

            List paths;
            paths.values.emplace_back(std::move(path));
            row.values.emplace_back("6");
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }
        {
            Row row;
            List paths;

            // 2->6->7
            {
                Path path;
                path.src = Vertex("2", {});
                path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("7", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
            }
            // 3->4->7
            {
                Path path;
                path.src = Vertex("3", {});
                path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("7", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
            }
            // 1->5->7
            {
                Path path;
                path.src = Vertex("1", {});
                path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("7", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
            }
            // 1->6->7
            {
                Path path;
                path.src = Vertex("1", {});
                path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("7", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
            }

            row.values.emplace_back("7");
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }

        auto resultDs = result.value().getDataSet();
        EXPECT_TRUE(verifyAllPaths(resultDs, expected));
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    // Step3
    {
        ResultBuilder builder;
        List datasets;
        datasets.values.emplace_back(std::move(thridStepResult_));
        builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
        qctx_->ectx()->setResult("input", builder.finish());

        auto future = allPathsExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(allPathsNode->outputVar());

        DataSet expected;
        expected.colNames = {"_dst", "_paths"};
        {
            // 0->1->5->7, 0->1->6->7
            List paths;
            {
                Path path;
                path.src = Vertex("0", {});
                path.steps.emplace_back(Step(Vertex("1", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("7", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
            }
            {
                Path path;
                path.src = Vertex("0", {});
                path.steps.emplace_back(Step(Vertex("1", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("7", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
            }
            Row row;
            row.values.emplace_back("7");
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }

        auto resultDs = result.value().getDataSet();
        EXPECT_TRUE(verifyAllPaths(resultDs, expected));
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
}

TEST_F(ProduceAllPathsTest, EmptyInput) {
    auto* allPathsNode = ProduceAllPaths::make(qctx_.get(), nullptr);
    allPathsNode->setInputVar("empty_get_neighbors");
    allPathsNode->setColNames({kDst, "_paths"});

    auto allPathsExe = std::make_unique<ProduceAllPathsExecutor>(allPathsNode, qctx_.get());
    auto future = allPathsExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(allPathsNode->outputVar());

    DataSet expected;
    expected.colNames = {"_dst", "_paths"};
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}
}  // namespace graph
}  // namespace nebula
