/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "planner/plan/Algo.h"
#include "executor/algo/ProduceSemiShortestPathExecutor.h"

namespace nebula {
namespace graph {

class ProduceSemiShortestPathTest : public testing::Test {
protected:
    static bool compareShortestPath(Row& row1, Row& row2) {
        // row : dst | src | cost | {paths}
        if (row1.values[0] != row2.values[0]) {
            return row1.values[0] < row2.values[0];
        }
        if (row1.values[1] != row2.values[1]) {
            return row1.values[1] < row2.values[1];
        }
        if (row1.values[2] != row2.values[2]) {
            return row1.values[2] < row2.values[2];
        }
        auto& pathList1 = row1.values[3].getList();
        auto& pathList2 = row2.values[3].getList();
        if (pathList1.size() != pathList2.size()) {
            return pathList1.size() < pathList2.size();
        }
        for (size_t i = 0; i < pathList1.size(); i++) {
            if (pathList1.values[i] != pathList2.values[i]) {
                return pathList1.values[i] < pathList2.values[i];
            }
        }
        return false;
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

TEST_F(ProduceSemiShortestPathTest, ShortestPath) {
    qctx_->symTable()->newVariable("input");

    auto* pssp = ProduceSemiShortestPath::make(qctx_.get(), nullptr);
    pssp->setInputVar("input");
    pssp->setColNames({"_dst", "_src", "cost", "paths"});

    auto psspExe = std::make_unique<ProduceSemiShortestPathExecutor>(pssp, qctx_.get());
    // Step 1
    {
        ResultBuilder builder;
        List datasets;
        datasets.values.emplace_back(std::move(firstStepResult_));
        builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
        qctx_->ectx()->setResult("input", builder.finish());

        auto future = psspExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(pssp->outputVar());

        DataSet expected;
        expected.colNames = {"_dst", "_src", "cost", "paths"};
        auto cost = 1;
        {
            // 0->1
            Row row;
            Path path;
            path.src = Vertex("0", {});
            path.steps.emplace_back(Step(Vertex("1", {}), 1, "edge1", 0, {}));

            List paths;
            paths.values.emplace_back(std::move(path));
            row.values.emplace_back("1");
            row.values.emplace_back("0");
            row.values.emplace_back(cost);
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
            row.values.emplace_back("1");
            row.values.emplace_back(cost);
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }
        {
            // 1->6
            Row row;
            Path path;
            path.src = Vertex("1", {});
            path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));

            List paths;
            paths.values.emplace_back(std::move(path));
            row.values.emplace_back("6");
            row.values.emplace_back("1");
            row.values.emplace_back(cost);
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }
        {
            // 2->6
            Row row;
            Path path;
            path.src = Vertex("2", {});
            path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));

            List paths;
            paths.values.emplace_back(std::move(path));
            row.values.emplace_back("6");
            row.values.emplace_back("2");
            row.values.emplace_back(cost);
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
            row.values.emplace_back("3");
            row.values.emplace_back(cost);
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }

        std::sort(expected.rows.begin(), expected.rows.end(), compareShortestPath);
        auto resultDs = result.value().getDataSet();
        std::sort(resultDs.rows.begin(), resultDs.rows.end(), compareShortestPath);
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

        auto future = psspExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(pssp->outputVar());

        DataSet expected;
        expected.colNames = {"_dst", "_src", "cost", "paths"};
        auto cost = 2;
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
            row.values.emplace_back("0");
            row.values.emplace_back(cost);
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
            row.values.emplace_back("0");
            row.values.emplace_back(cost);
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }
        {
            // 2->6->7
            Row row;
            Path path;
            path.src = Vertex("2", {});
            path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("7", {}), 1, "edge1", 0, {}));

            List paths;
            paths.values.emplace_back(std::move(path));
            row.values.emplace_back("7");
            row.values.emplace_back("2");
            row.values.emplace_back(cost);
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }
        {
            // 3->4->7
            Row row;
            Path path;
            path.src = Vertex("3", {});
            path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("7", {}), 1, "edge1", 0, {}));

            List paths;
            paths.values.emplace_back(std::move(path));
            row.values.emplace_back("7");
            row.values.emplace_back("3");
            row.values.emplace_back(cost);
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }
        {
            // 1->5->7, 1->6->7
            List paths;
            {
                Path path;
                path.src = Vertex("1", {});
                path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("7", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
            }
            {
                Path path;
                path.src = Vertex("1", {});
                path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("7", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
            }
            Row row;
            row.values.emplace_back("7");
            row.values.emplace_back("1");
            row.values.emplace_back(cost);
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }

        std::sort(expected.rows.begin(), expected.rows.end(), compareShortestPath);
        auto resultDs = result.value().getDataSet();
        std::sort(resultDs.rows.begin(), resultDs.rows.end(), compareShortestPath);
        EXPECT_EQ(resultDs, expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    // Step3
    {
        ResultBuilder builder;
        List datasets;
        datasets.values.emplace_back(std::move(thridStepResult_));
        builder.value(std::move(datasets)).iter(Iterator::Kind::kGetNeighbors);
        qctx_->ectx()->setResult("input", builder.finish());

        auto future = psspExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(pssp->outputVar());

        DataSet expected;
        expected.colNames = {"_dst", "_src", "cost", "paths"};
        auto cost = 3;
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
            row.values.emplace_back("0");
            row.values.emplace_back(cost);
            row.values.emplace_back(std::move(paths));
            expected.rows.emplace_back(std::move(row));
        }

        std::sort(expected.rows.begin(), expected.rows.end(), compareShortestPath);
        auto resultDs = result.value().getDataSet();
        std::sort(resultDs.rows.begin(), resultDs.rows.end(), compareShortestPath);
        EXPECT_EQ(resultDs, expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
}

TEST_F(ProduceSemiShortestPathTest, EmptyInput) {
    auto* pssp = ProduceSemiShortestPath::make(qctx_.get(), nullptr);
    pssp->setInputVar("empty_get_neighbors");
    pssp->setColNames({"_dst", "_src", "cost", "paths"});

    auto psspExe = std::make_unique<ProduceSemiShortestPathExecutor>(pssp, qctx_.get());
    auto future = psspExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(pssp->outputVar());

    DataSet expected;
    expected.colNames = {"_dst", "_src", "cost", "paths"};
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

}   // namespace graph
}   // namespace nebula
