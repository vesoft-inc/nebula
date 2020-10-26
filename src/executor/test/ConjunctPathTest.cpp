/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/algo/ConjunctPathExecutor.h"
#include "planner/Algo.h"
#include "planner/Logic.h"

namespace nebula {
namespace graph {
class ConjunctPathTest : public testing::Test {
protected:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        qctx_->symTable()->newVariable("forward1");
        qctx_->symTable()->newVariable("backward1");
        qctx_->symTable()->newVariable("backward2");
        qctx_->symTable()->newVariable("backward3");
        qctx_->symTable()->newVariable("backward4");
        {
            // 1->2
            // 1->3
            DataSet ds1;
            ds1.colNames = {kVid, "edge"};
            {
                Row row;
                row.values = {"1", Value::kEmpty};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("forward1", ResultBuilder().value(ds1).finish());

            DataSet ds2;
            ds2.colNames = {kVid, "edge"};
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
            ds1.colNames = {kVid, "edge"};
            {
                Row row;
                row.values = {"4", Value::kEmpty};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward1", ResultBuilder().value(ds1).finish());

            DataSet ds2;
            ds2.colNames = {kVid, "edge"};
            qctx_->ectx()->setResult("backward1", ResultBuilder().value(ds2).finish());
        }
        {
            // 2
            DataSet ds1;
            ds1.colNames = {kVid, "edge"};
            {
                Row row;
                row.values = {"2", Value::kEmpty};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward2", ResultBuilder().value(ds1).finish());

            DataSet ds2;
            ds2.colNames = {kVid, "edge"};
            qctx_->ectx()->setResult("backward2", ResultBuilder().value(ds2).finish());
        }
        {
            // 4->3
            DataSet ds1;
            ds1.colNames = {kVid, "edge"};
            {
                Row row;
                row.values = {"4", Value::kEmpty};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward3", ResultBuilder().value(ds1).finish());

            DataSet ds2;
            ds2.colNames = {kVid, "edge"};
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
            ds1.colNames = {kVid, "edge"};
            {
                Row row;
                row.values = {"5", Value::kEmpty};
                ds1.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward4", ResultBuilder().value(ds1).finish());

            DataSet ds2;
            ds2.colNames = {kVid, "edge"};
            {
                Row row;
                row.values = {"4", Edge("5", "4", -1, "edge1", 0, {})};
                ds2.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("backward4", ResultBuilder().value(ds2).finish());
        }

        qctx_->symTable()->newVariable("all_paths_forward1");
        qctx_->symTable()->newVariable("all_paths_backward1");
        qctx_->symTable()->newVariable("all_paths_backward2");
        qctx_->symTable()->newVariable("all_paths_backward3");
        qctx_->symTable()->newVariable("all_paths_backward4");
        {
            // 1->2
            // 1->3
            DataSet ds;
            ds.colNames = {kVid, "path"};
            {
                Row row;
                List paths;
                Path path;
                path.src = Vertex("1", {});
                path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
                row.values = {"2", std::move(paths)};
                ds.rows.emplace_back(std::move(row));
            }
            {
                Row row;
                List paths;
                Path path;
                path.src = Vertex("1", {});
                path.steps.emplace_back(Step(Vertex("3", {}), 1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
                row.values = {"3", std::move(paths)};
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("all_paths_forward1", ResultBuilder().value(ds).finish());
        }
        {
            // 4->7
            DataSet ds2;
            ds2.colNames = {kVid, "path"};
            {
                Row row;
                List paths;
                Path path;
                path.src = Vertex("4", {});
                path.steps.emplace_back(Step(Vertex("7", {}), -1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
                row.values = {"7", std::move(paths)};
                ds2.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("all_paths_backward1", ResultBuilder().value(ds2).finish());
        }
        {
            // 2
            DataSet ds1;
            ds1.colNames = {kVid, "path"};
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
            ds2.colNames = {kVid, "path"};
            {
                Row row;
                List paths;
                Path path;
                path.src = Vertex("2", {});
                path.steps.emplace_back(Step(Vertex("7", {}), -1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
                row.values = {"7", std::move(paths)};
                ds2.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("all_paths_backward2", ResultBuilder().value(ds2).finish());
        }
        {
            // 4->3
            DataSet ds2;
            ds2.colNames = {kVid, "path"};
            {
                Row row;
                List paths;
                Path path;
                path.src = Vertex("4", {});
                path.steps.emplace_back(Step(Vertex("3", {}), -1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
                row.values = {"3", std::move(paths)};
                ds2.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("all_paths_backward3", ResultBuilder().value(ds2).finish());
        }
        {
            // 5->4
            DataSet ds;
            ds.colNames = {kVid, "path"};
            {
                Row row;
                List paths;
                Path path;
                path.src = Vertex("5", {});
                path.steps.emplace_back(Step(Vertex("4", {}), -1, "edge1", 0, {}));
                paths.values.emplace_back(std::move(path));
                row.values = {"4", std::move(paths)};
                ds.rows.emplace_back(std::move(row));
            }
            qctx_->ectx()->setResult("all_paths_backward4", ResultBuilder().value(ds).finish());
        }
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
    conjunct->setColNames({"_path"});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {"_path"};
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
    conjunct->setColNames({"_path"});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {"_path"};
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
    conjunct->setColNames({"_path"});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {"_path"};
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
    conjunct->setColNames({"_path"});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());

    {
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {"_path"};
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    {
        {
            // 2->4@0
            // 2->4@1
            DataSet ds1;
            ds1.colNames = {kVid, "edge"};
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
            ds1.colNames = {kVid, "edge"};
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
        expected.colNames = {"_path"};
        {
            Row row;
            Path path;
            path.src = Vertex("1", {});
            path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));
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
    conjunct->setColNames({"_path"});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());

    {
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {"_path"};
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    {
        {
            // 2->6@0
            // 2->6@1
            DataSet ds1;
            ds1.colNames = {kVid, "edge"};
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
            ds1.colNames = {kVid, "edge"};
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
        expected.colNames = {"_path"};
        {
            Row row;
            Path path;
            path.src = Vertex("1", {});
            path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));
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
    conjunct->setColNames({"_path"});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {"_path"};
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
    conjunct->setColNames({"_path"});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {"_path"};
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
    conjunct->setColNames({"_path"});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());
    auto future = conjunctExe->execute();
    auto status = std::move(future).get();
    EXPECT_TRUE(status.ok());
    auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

    DataSet expected;
    expected.colNames = {"_path"};
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

TEST_F(ConjunctPathTest, AllPathsThreeStepsPath) {
    auto* conjunct = ConjunctPath::make(qctx_.get(),
                                        StartNode::make(qctx_.get()),
                                        StartNode::make(qctx_.get()),
                                        ConjunctPath::PathKind::kAllPaths,
                                        5);
    conjunct->setLeftVar("all_paths_forward1");
    conjunct->setRightVar("all_paths_backward4");
    conjunct->setColNames({"_path"});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());

    {
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {"_path"};
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    {
        {
            // 1->2->4@0
            // 1->2->4@1
            DataSet ds1;
            ds1.colNames = {kVid, "path"};

            Row row;
            List paths;
            {
                Path path;
                path.src = Vertex("1", {});
                path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 0, {}));
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
        expected.colNames = {"_path"};
        {
            Row row;
            Path path;
            path.src = Vertex("1", {});
            path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));
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
    conjunct->setColNames({"_path"});

    auto conjunctExe = std::make_unique<ConjunctPathExecutor>(conjunct, qctx_.get());

    {
        auto future = conjunctExe->execute();
        auto status = std::move(future).get();
        EXPECT_TRUE(status.ok());
        auto& result = qctx_->ectx()->getResult(conjunct->outputVar());

        DataSet expected;
        expected.colNames = {"_path"};
        EXPECT_EQ(result.value().getDataSet(), expected);
        EXPECT_EQ(result.state(), Result::State::kSuccess);
    }

    {
        {
            // 1->2->6@0
            // 1->2->6@1
            DataSet ds1;
            ds1.colNames = {kVid, "path"};

            Row row;
            List paths;
            {
                Path path;
                path.src = Vertex("1", {});
                path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
                path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));
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
            ds1.colNames = {kVid, "path"};

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
        expected.colNames = {"_path"};
        {
            Row row;
            Path path;
            path.src = Vertex("1", {});
            path.steps.emplace_back(Step(Vertex("2", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("6", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("4", {}), 1, "edge1", 0, {}));
            path.steps.emplace_back(Step(Vertex("5", {}), 1, "edge1", 0, {}));
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
}  // namespace graph
}  // namespace nebula
