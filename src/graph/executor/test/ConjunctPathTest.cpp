/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/algo/ConjunctPathExecutor.h"
#include "planner/plan/Algo.h"
#include "planner/plan/Logic.h"

namespace nebula {
namespace graph {
class ConjunctPathTest : public testing::Test {
protected:
    Path createPath(const std::string& src, const std::vector<std::string>& steps, int type) {
        Path path;
        path.src = Vertex(src, {});
        for (auto& i : steps) {
            path.steps.emplace_back(Step(Vertex(i, {}), type, "edge1", 0, {}));
        }
        return path;
    }
    static bool comparePath(Row& row1, Row& row2) {
        // row : path|  cost |
        if (row1.values[1] != row2.values[1]) {
            return row1.values[0] < row2.values[0];
        }
        return row1[0] < row2[0];
    }
    void multiplePairPathInit() {
        /*
         *  overall path is :
         *  0->1->5->7->8->9
         *  1->6->7
         *  2->6->7
         *  3->4->7
         *  7->10->11->12
         *  startVids {0, 1, 2, 3}
         *  endVids {9, 12}
         */
        qctx_->symTable()->newVariable("forwardPath1");
        qctx_->symTable()->newVariable("forwardPath2");
        qctx_->symTable()->newVariable("forwardPath3");
        qctx_->symTable()->newVariable("backwardPath1");
        qctx_->symTable()->newVariable("backwardPath2");
        qctx_->symTable()->newVariable("backwardPath3");
        qctx_->symTable()->newVariable("conditionalVar");
        {
            DataSet ds;
            ds.colNames = {kVid, kVid};
            std::vector<std::string> dst = {"9", "12"};
            for (size_t i = 0; i < 4; i++) {
                for (size_t j = 0; j < dst.size(); ++j) {
                    Row row;
                    row.values.emplace_back(folly::to<std::string>(i));
                    row.values.emplace_back(dst[j]);
                    ds.rows.emplace_back(std::move(row));
                }
            }
            qctx_->ectx()->setResult("conditionalVar", ResultBuilder().value(ds).finish());
        }
        {
            DataSet ds;
            auto cost = 1;
            ds.colNames = {kDst, kSrc, kCostStr, kPathStr};
            {
                // 0->1
                Row row;
                Path path = createPath("0", {"1"}, 1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("1");
                row.values.emplace_back("0");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 1->5
                Row row;
                Path path = createPath("1", {"5"}, 1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("5");
                row.values.emplace_back("1");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 1->6
                Row row;
                Path path = createPath("1", {"6"}, 1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("6");
                row.values.emplace_back("1");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 2->6
                Row row;
                Path path = createPath("2", {"6"}, 1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("6");
                row.values.emplace_back("2");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 3->4
                Row row;
                Path path = createPath("3", {"4"}, 1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("4");
                row.values.emplace_back("3");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("forwardPath1", ResultBuilder().value(ds).finish());
        }
        {
            DataSet ds;
            auto cost = 2;
            ds.colNames = {kDst, kSrc, kCostStr, kPathStr};
            {
                // 0->1->5
                Row row;
                Path path = createPath("0", {"1", "5"}, 1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("5");
                row.values.emplace_back("0");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 0->1->6
                Row row;
                Path path = createPath("0", {"1", "6"}, 1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("6");
                row.values.emplace_back("0");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 2->6->7
                Row row;
                Path path = createPath("2", {"6", "7"}, 1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("7");
                row.values.emplace_back("2");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 3->4->7
                Row row;
                Path path = createPath("3", {"4", "7"}, 1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("7");
                row.values.emplace_back("3");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 1->5->7, 1->6->7
                List paths;
                for (auto i = 5; i < 7; i++) {
                    Path path = createPath("1", {folly::to<std::string>(i), "7"}, 1);
                    paths.values.emplace_back(std::move(path));
                }
                Row row;
                row.values.emplace_back("7");
                row.values.emplace_back("1");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("forwardPath2", ResultBuilder().value(ds).finish());
        }
        {
            DataSet ds;
            auto cost = 3;
            ds.colNames = {kDst, kSrc, kCostStr, kPathStr};
            {
                // 0->1->5->7, 0->1->6->7
                List paths;
                for (auto i = 5; i < 7; i++) {
                    Path path = createPath("0", {"1", folly::to<std::string>(i), "7"}, 1);
                    paths.values.emplace_back(std::move(path));
                }
                Row row;
                row.values.emplace_back("7");
                row.values.emplace_back("0");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 1->5->7->8, 1->6->7->8
                List paths;
                for (auto i = 5; i < 7; i++) {
                    Path path = createPath("1", {folly::to<std::string>(i), "7", "8"}, 1);
                    paths.values.emplace_back(std::move(path));
                }
                Row row;
                row.values.emplace_back("8");
                row.values.emplace_back("1");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 2->6->7->8
                List paths;
                Path path = createPath("2", {"6", "7", "8"}, 1);
                paths.values.emplace_back(std::move(path));
                Row row;
                row.values.emplace_back("8");
                row.values.emplace_back("2");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 3->4->7->8
                List paths;
                Path path = createPath("3", {"4", "7", "8"}, 1);
                paths.values.emplace_back(std::move(path));
                Row row;
                row.values.emplace_back("8");
                row.values.emplace_back("3");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 1->5->7->10, 1->6->7->10
                List paths;
                for (auto i = 5; i < 7; i++) {
                    Path path = createPath("1", {folly::to<std::string>(i), "7", "10"}, 1);
                    paths.values.emplace_back(std::move(path));
                }
                Row row;
                row.values.emplace_back("10");
                row.values.emplace_back("1");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 2->6->7->10
                List paths;
                Path path = createPath("2", {"6", "7", "10"}, 1);
                paths.values.emplace_back(std::move(path));
                Row row;
                row.values.emplace_back("10");
                row.values.emplace_back("2");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 3->4->7->10
                List paths;
                Path path = createPath("3", {"4", "7", "10"}, 1);
                paths.values.emplace_back(std::move(path));
                Row row;
                row.values.emplace_back("10");
                row.values.emplace_back("3");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("forwardPath3", ResultBuilder().value(ds).finish());
        }
        // backward endVid {9, 12}
        {
            DataSet ds;
            ds.colNames = {kDst, kSrc, kCostStr, kPathStr};
            {
                Row row;
                row.values = {"9", Value::kEmpty, 0, Value::kEmpty};
                ds.rows.emplace_back(std::move(row));
            }
            {
                Row row;
                row.values = {"12", Value::kEmpty, 0, Value::kEmpty};
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backwardPath1", ResultBuilder().value(ds).finish());

            DataSet ds1;
            auto cost = 1;
            ds1.colNames = {kDst, kSrc, kCostStr, kPathStr};
            {
                // 9->8
                Row row;
                Path path = createPath("9", {"8"}, -1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("8");
                row.values.emplace_back("9");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds1.rows.emplace_back(std::move(row));
            }
            {
                // 12->11
                Row row;
                Path path = createPath("12", {"11"}, -1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("11");
                row.values.emplace_back("12");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backwardPath1", ResultBuilder().value(ds1).finish());
            qctx_->ectx()->setResult("backwardPath2", ResultBuilder().value(ds1).finish());
        }
        {
            DataSet ds;
            auto cost = 2;
            ds.colNames = {kDst, kSrc, kCostStr, kPathStr};
            {
                // 9->8->7
                Row row;
                Path path = createPath("9", {"8", "7"}, -1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("7");
                row.values.emplace_back("9");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            {
                // 12->11->10
                Row row;
                Path path = createPath("12", {"11", "10"}, -1);

                List paths;
                paths.values.emplace_back(std::move(path));
                row.values.emplace_back("10");
                row.values.emplace_back("12");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backwardPath2", ResultBuilder().value(ds).finish());
            qctx_->ectx()->setResult("backwardPath3", ResultBuilder().value(ds).finish());
        }
        {
            DataSet ds;
            auto cost = 3;
            ds.colNames = {kDst, kSrc, kCostStr, kPathStr};
            {
                // 9->8->7->4, 9->8->7->5, 9->8->7->6
                for (auto i = 4; i < 7; i++) {
                    Row row;
                    Path path = createPath("9", {"8", "7", folly::to<std::string>(i)}, -1);

                    List paths;
                    paths.values.emplace_back(std::move(path));
                    row.values.emplace_back(folly::to<std::string>(i));
                    row.values.emplace_back("9");
                    row.values.emplace_back(cost);
                    row.values.emplace_back(std::move(paths));
                    ds.rows.emplace_back(std::move(row));
                }
            }
            {
                // 12->11->10->7
                List paths;
                Path path = createPath("12", {"11", "10", "7"}, -1);
                paths.values.emplace_back(std::move(path));
                Row row;
                row.values.emplace_back("7");
                row.values.emplace_back("12");
                row.values.emplace_back(cost);
                row.values.emplace_back(std::move(paths));
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backwardPath3", ResultBuilder().value(ds).finish());
        }
    }
    void biBfsInit() {
        qctx_->symTable()->newVariable("forward1");
        qctx_->symTable()->newVariable("backward1");
        qctx_->symTable()->newVariable("backward2");
        qctx_->symTable()->newVariable("backward3");
        qctx_->symTable()->newVariable("backward4");
        {
            // 1->2
            // 1->3
            DataSet ds1;
            ds1.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"1", Value::kEmpty};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("forward1", ResultBuilder().value(ds1).finish());

            DataSet ds2;
            ds2.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"2", Edge("1", "2", 1, "edge1", 0, {})};
                ds2.rows.emplace_back(std::move(row));
            }
            {
                Row row;
                row.values = {"3", Edge("1", "3", 1, "edge1", 0, {})};
                ds2.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("forward1", ResultBuilder().value(ds2).finish());
        }
        {
            // 4
            DataSet ds1;
            ds1.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"4", Value::kEmpty};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward1", ResultBuilder().value(ds1).finish());

            DataSet ds2;
            ds2.colNames = {kVid, kEdgeStr};
            qctx_->ectx()->setResult("backward1", ResultBuilder().value(ds2).finish());
        }
        {
            // 2
            DataSet ds1;
            ds1.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"2", Value::kEmpty};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward2", ResultBuilder().value(ds1).finish());

            DataSet ds2;
            ds2.colNames = {kVid, kEdgeStr};
            qctx_->ectx()->setResult("backward2", ResultBuilder().value(ds2).finish());
        }
        {
            // 4->3
            DataSet ds1;
            ds1.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"4", Value::kEmpty};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward3", ResultBuilder().value(ds1).finish());

            DataSet ds2;
            ds2.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"3", Edge("4", "3", -1, "edge1", 0, {})};
                ds2.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward3", ResultBuilder().value(ds2).finish());
        }
        {
            // 5->4
            DataSet ds1;
            ds1.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"5", Value::kEmpty};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward4", ResultBuilder().value(ds1).finish());

            DataSet ds2;
            ds2.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"4", Edge("5", "4", -1, "edge1", 0, {})};
                ds2.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward4", ResultBuilder().value(ds2).finish());
        }
    }

    void allPathInit() {
        qctx_->symTable()->newVariable("all_paths_forward1");
        qctx_->symTable()->newVariable("all_paths_backward1");
        qctx_->symTable()->newVariable("all_paths_backward2");
        qctx_->symTable()->newVariable("all_paths_backward3");
        qctx_->symTable()->newVariable("all_paths_backward4");
        {
            // 1->2
            // 1->3
            DataSet ds;
            ds.colNames = {kVid, kPathStr};
            {
                Row row;
                List paths;
                Path path = createPath("1", {"2"}, 1);
                paths.values.emplace_back(std::move(path));
                row.values = {"2", std::move(paths)};
                ds.rows.emplace_back(std::move(row));
            }
            {
                Row row;
                List paths;
                Path path = createPath("1", {"3"}, 1);
                paths.values.emplace_back(std::move(path));
                row.values = {"3", std::move(paths)};
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("all_paths_forward1", ResultBuilder().value(ds).finish());
        }
        {
            // 4->7
            DataSet ds2;
            ds2.colNames = {kVid, kPathStr};
            {
                Row row;
                List paths;
                Path path = createPath("4", {"7"}, -1);
                paths.values.emplace_back(std::move(path));
                row.values = {"7", std::move(paths)};
                ds2.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("all_paths_backward1", ResultBuilder().value(ds2).finish());
        }
        {
            // 2
            DataSet ds1;
            ds1.colNames = {kVid, kPathStr};
            {
                Row row;
                List paths;
                Path path;
                path.src = Vertex("2", {});
                paths.values.emplace_back(std::move(path));
                row.values = {"2", std::move(paths)};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("all_paths_backward2", ResultBuilder().value(ds1).finish());

            // 2->7
            DataSet ds2;
            ds2.colNames = {kVid, kPathStr};
            {
                Row row;
                List paths;
                Path path = createPath("2", {"7"}, -1);
                paths.values.emplace_back(std::move(path));
                row.values = {"7", std::move(paths)};
                ds2.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("all_paths_backward2", ResultBuilder().value(ds2).finish());
        }
        {
            // 4->3
            DataSet ds2;
            ds2.colNames = {kVid, kPathStr};
            {
                Row row;
                List paths;
                Path path = createPath("4", {"3"}, -1);
                paths.values.emplace_back(std::move(path));
                row.values = {"3", std::move(paths)};
                ds2.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("all_paths_backward3", ResultBuilder().value(ds2).finish());
        }
        {
            // 5->4
            DataSet ds;
            ds.colNames = {kVid, kPathStr};
            {
                Row row;
                List paths;
                Path path = createPath("5", {"4"}, -1);
                paths.values.emplace_back(std::move(path));
                row.values = {"4", std::move(paths)};
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("all_paths_backward4", ResultBuilder().value(ds).finish());
        }
    }

    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        biBfsInit();
        multiplePairPathInit();
        allPathInit();
    }

protected:
    std::unique_ptr<QueryContext> qctx_;
};

TEST_F(ConjunctPathTest, BiBFSNoPath) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kBiBFS,
                                        5);
    conjunct->setLeftVar("forward1");
    conjunct->setRightVar("backward1");
    conjunct->setColNames({kPathStr});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {kPathStr};
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(ConjunctPathTest, BiBFSOneStepPath) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kBiBFS,
                                        5);
    conjunct->setLeftVar("forward1");
    conjunct->setRightVar("backward2");
    conjunct->setColNames({kPathStr});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {kPathStr};
    Row row;
    Path path;
    path.src = Vertex("1", {});
    path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
    row.values.emplace_back(std::move(path));
    expected.rows.emplace_back(std::move(row));

    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(ConjunctPathTest, BiBFSTwoStepsPath) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kBiBFS,
                                        5);
    conjunct->setLeftVar("forward1");
    conjunct->setRightVar("backward3");
    conjunct->setColNames({kPathStr});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {kPathStr};
    Row row;
    Path path;
    path.src = Vertex("1", {});
    path.steps.emplace_back(Step(Vertex("3", {}), 1, "edge1", 0, {}));
    path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 0, {}));
    row.values.emplace_back(std::move(path));
    expected.rows.emplace_back(std::move(row));

    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(ConjunctPathTest, BiBFSThreeStepsPath) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kBiBFS,
                                        5);
    conjunct->setLeftVar("forward1");
    conjunct->setRightVar("backward4");
    conjunct->setColNames({kPathStr});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());

    {
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {kPathStr};
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    {
        {
            // 2->4@0
            // 2->4@1
            DataSet ds1;
            ds1.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"4", Edge("2", "4", 1, "edge1", 0, {})};
                ds1.rows.emplace_back(std::move(row));
            }
            {
                Row row;
                row.values = {"4", Edge("2", "4", 1, "edge1", 1, {})};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("forward1", ResultBuilder().value(ds1).finish());
        }
        {
            // 4->6@0
            DataSet ds1;
            ds1.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"6", Edge("4", "6", -1, "edge1", 0, {})};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward4", ResultBuilder().value(ds1).finish());
        }
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {kPathStr};
        {
            Row row;
            Path path = createPath("1", {"2", "4", "5"}, 1);
            row.values.emplace_back(std::move(path));
            expected.rows.emplace_back(std::move(row));
        }
        {
            Row row;
            Path path;
            path.src = Vertex("1", {});
            path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 1, {}));
            path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));
            row.values.emplace_back(std::move(path));
            expected.rows.emplace_back(std::move(row));
        }

        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
}

TEST_F(ConjunctPathTest, BiBFSFourStepsPath) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kBiBFS,
                                        5);
    conjunct->setLeftVar("forward1");
    conjunct->setRightVar("backward4");
    conjunct->setColNames({kPathStr});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());

    {
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {kPathStr};
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    {
        {
            // 2->6@0
            // 2->6@1
            DataSet ds1;
            ds1.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"6", Edge("2", "6", 1, "edge1", 0, {})};
                ds1.rows.emplace_back(std::move(row));
            }
            {
                Row row;
                row.values = {"6", Edge("2", "6", 1, "edge1", 1, {})};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("forward1", ResultBuilder().value(ds1).finish());
        }
        {
            // 4->6@0
            DataSet ds1;
            ds1.colNames = {kVid, kEdgeStr};
            {
                Row row;
                row.values = {"6", Edge("4", "6", -1, "edge1", 0, {})};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward4", ResultBuilder().value(ds1).finish());
        }
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {kPathStr};
        {
            Row row;
            Path path = createPath("1", {"2", "6", "4", "5"}, 1);
            row.values.emplace_back(std::move(path));
            expected.rows.emplace_back(std::move(row));
        }
        {
            Row row;
            Path path;
            path.src = Vertex("1", {});
            path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 1, {}));
            path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));
            row.values.emplace_back(std::move(path));
            expected.rows.emplace_back(std::move(row));
        }

        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
}

TEST_F(ConjunctPathTest, AllPathsNoPath) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kAllPaths,
                                        5);
    conjunct->setLeftVar("all_paths_forward1");
    conjunct->setRightVar("all_paths_backward1");
    conjunct->setColNames({kPathStr});
    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());
    DataSet expected;
    expected.colNames = {kPathStr};
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(ConjunctPathTest, AllPathsOneStepPath) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kAllPaths,
                                        5);
    conjunct->setLeftVar("all_paths_forward1");
    conjunct->setRightVar("all_paths_backward2");
    conjunct->setColNames({kPathStr});
    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {kPathStr};
    Row row;
    Path path;
    path.src = Vertex("1", {});
    path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
    row.values.emplace_back(std::move(path));
    expected.rows.emplace_back(std::move(row));
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(ConjunctPathTest, AllPathsTwoStepsPath) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kAllPaths,
                                        5);
    conjunct->setLeftVar("all_paths_forward1");
    conjunct->setRightVar("all_paths_backward3");
    conjunct->setColNames({kPathStr});
    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {kPathStr};
    Row row;
    Path path = createPath("1", {"3", "4"}, 1);
    row.values.emplace_back(std::move(path));
    expected.rows.emplace_back(std::move(row));

    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(ConjunctPathTest, AllPathsThreeStepsPath) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kAllPaths,
                                        5);
    conjunct->setLeftVar("all_paths_forward1");
    conjunct->setRightVar("all_paths_backward4");
    conjunct->setColNames({kPathStr});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());

    {
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {kPathStr};
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    {
        {
            // 1->2->4@0
            // 1->2->4@1
            DataSet ds1;
            ds1.colNames = {kVid, kPathStr};

            Row row;
            List paths;
            {
                Path path = createPath("1", {"2", "4"}, 1);
                paths.values.emplace_back(std::move(path));
            }
            {
                Path path;
                path.src = Vertex("1", {});
                path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 1, {}));
                paths.values.emplace_back(std::move(path));
            }
            row.values = {"4", std::move(paths)};
            ds1.rows.emplace_back(std::move(row));
            qctx_->ectx()->setResult("all_paths_forward1", ResultBuilder().value(ds1).finish());
        }
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {kPathStr};
        {
            Row row;
            Path path = createPath("1", {"2", "4", "5"}, 1);
            row.values.emplace_back(std::move(path));
            expected.rows.emplace_back(std::move(row));
        }
        {
            Row row;
            Path path;
            path.src = Vertex("1", {});
            path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 1, {}));
            path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));
            row.values.emplace_back(std::move(path));
            expected.rows.emplace_back(std::move(row));
        }

        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
}

TEST_F(ConjunctPathTest, AllPathsFourStepsPath) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kAllPaths,
                                        5);
    conjunct->setLeftVar("all_paths_forward1");
    conjunct->setRightVar("all_paths_backward4");
    conjunct->setColNames({kPathStr});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());

    {
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {kPathStr};
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    {
        {
            // 1->2->6@0
            // 1->2->6@1
            DataSet ds1;
            ds1.colNames = {kVid, kPathStr};

            Row row;
            List paths;
            {
                Path path = createPath("1", {"2", "6"}, 1);
                paths.values.emplace_back(std::move(path));
            }
            {
                Path path;
                path.src = Vertex("1", {});
                path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 1, {}));
                paths.values.emplace_back(std::move(path));
            }
            row.values = {"6", std::move(paths)};
            ds1.rows.emplace_back(std::move(row));
            qctx_->ectx()->setResult("all_paths_forward1", ResultBuilder().value(ds1).finish());
        }
        {
            // 5->4->6@0
            DataSet ds1;
            ds1.colNames = {kVid, kPathStr};

            Row row;
            List paths;
            {
                Path path;
                path.src = Vertex("5", {});
                path.steps.emplace_back(Step(Vertex("4", {}), -1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("6", {}), -1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
            }
            row.values = {"6", std::move(paths)};
            ds1.rows.emplace_back(std::move(row));
            qctx_->ectx()->setResult("all_paths_backward4", ResultBuilder().value(ds1).finish());
        }
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {kPathStr};
        {
            Row row;
            Path path = createPath("1", {"2", "6", "4", "5"}, 1);
            row.values.emplace_back(std::move(path));
            expected.rows.emplace_back(std::move(row));
        }
        {
            Row row;
            Path path;
            path.src = Vertex("1", {});
            path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 1, {}));
            path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));
            row.values.emplace_back(std::move(path));
            expected.rows.emplace_back(std::move(row));
        }

        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }
}

TEST_F(ConjunctPathTest, multiplePairOneStep) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kFloyd,
                                        5);
    conjunct->setLeftVar("forwardPath1");
    conjunct->setRightVar("backwardPath1");
    conjunct->setColNames({kPathStr, kCostStr});
    conjunct->setConditionalVar("conditionalVar");
    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {kPathStr, kCostStr};
    EXPECT_EQ(result.value().getDataSet(), expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(ConjunctPathTest, multiplePairTwoSteps) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kFloyd,
                                        5);
    conjunct->setLeftVar("forwardPath2");
    conjunct->setRightVar("backwardPath2");
    conjunct->setColNames({kPathStr, kCostStr});
    conjunct->setConditionalVar("conditionalVar");
    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {kPathStr, kCostStr};
    {
        // 1->5->7->8->9, 1->6->7->8->9
        for (auto i = 5; i < 7; i++) {
            Row row;
            Path path = createPath("1", {folly::to<std::string>(i), "7", "8", "9"}, 1);
            row.values.emplace_back(std::move(path));
            row.values.emplace_back(4);
            expected.rows.emplace_back(std::move(row));
        }
    }
    {
        // 2->6->7->8->9
        Row row;
        Path path = createPath("2", {"6", "7", "8", "9"}, 1);
        row.values.emplace_back(std::move(path));
        row.values.emplace_back(4);
        expected.rows.emplace_back(std::move(row));
    }
    {
        // 3->4->7->8->9
        Row row;
        Path path = createPath("3", {"4", "7", "8", "9"}, 1);
        row.values.emplace_back(std::move(path));
        row.values.emplace_back(4);
        expected.rows.emplace_back(std::move(row));
    }
    std::sort(expected.rows.begin(), expected.rows.end(), comparePath);
    auto resultDs = result.value().getDataSet();
    std::sort(resultDs.rows.begin(), resultDs.rows.end(), comparePath);
    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

TEST_F(ConjunctPathTest, multiplePairThreeSteps) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kFloyd,
                                        5);
    conjunct->setLeftVar("forwardPath3");
    conjunct->setRightVar("backwardPath3");
    conjunct->setColNames({kPathStr, kCostStr});
    conjunct->setConditionalVar("conditionalVar");
    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {kPathStr, kCostStr};
    {
        // 1->5->7->10->11->12, 1->6->7->10->11->12
        for (auto i = 5; i < 7; i++) {
            Row row;
            Path path = createPath("1", {folly::to<std::string>(i), "7", "10", "11", "12"}, 1);
            row.values.emplace_back(std::move(path));
            row.values.emplace_back(5);
            expected.rows.emplace_back(std::move(row));
        }
    }
    {
        // 0->1->5->7->8->9, 0->1->6->7->8->9
        for (auto i = 5; i < 7; i++) {
            Row row;
            Path path = createPath("0", {"1", folly::to<std::string>(i), "7", "8", "9"}, 1);
            row.values.emplace_back(std::move(path));
            row.values.emplace_back(5);
            expected.rows.emplace_back(std::move(row));
        }
    }
    {
        // 2->6->7->10->11->12
        Row row;
        Path path = createPath("2", {"6", "7", "10", "11", "12"}, 1);
        row.values.emplace_back(std::move(path));
        row.values.emplace_back(5);
        expected.rows.emplace_back(std::move(row));
    }
    {
        // 3->4->7->10->11->12
        Row row;
        Path path = createPath("3", {"4", "7", "10", "11", "12"}, 1);
        row.values.emplace_back(std::move(path));
        row.values.emplace_back(5);
        expected.rows.emplace_back(std::move(row));
    }
    {
        // 0->1->5->7->10->11->12, 0->1->6->7->10->11->12
        for (auto i = 5; i < 7; i++) {
            Row row;
            Path path = createPath("0", {"1", folly::to<std::string>(i), "7", "10", "11", "12"}, 1);
            row.values.emplace_back(std::move(path));
            row.values.emplace_back(6);
            expected.rows.emplace_back(std::move(row));
        }
    }
    std::sort(expected.rows.begin(), expected.rows.end(), comparePath);
    auto resultDs = result.value().getDataSet();
    std::sort(resultDs.rows.begin(), resultDs.rows.end(), comparePath);
    EXPECT_EQ(resultDs, expected);
    EXPECT_EQ(result.state(), Result::State::kSuccess);
}

}  // namespace graph
}  // namespace nebula
