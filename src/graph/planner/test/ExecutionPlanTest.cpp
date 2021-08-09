/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/plan/ExecutionPlan.h"

#include <folly/init/Init.h>
#include <folly/stop_watch.h>
#include <gtest/gtest.h>

#include "context/QueryContext.h"
#include "executor/ExecutionError.h"
#include "executor/Executor.h"
#include "planner/plan/Query.h"
#include "scheduler/Scheduler.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;

namespace nebula {
namespace graph {

class ExecutionPlanTest : public ::testing::Test {
public:
    void SetUp() override {
        qctx_ = std::make_unique<QueryContext>();
        scheduler_ = std::make_unique<Scheduler>(qctx_.get());
        plan_ = qctx_->plan();
    }

    void run() {
        ASSERT_NE(plan_->root(), nullptr);

        watch_.reset();
        scheduler_->schedule()
            .thenValue([](Status s) { ASSERT_TRUE(s.ok()) << s.toString(); })
            .onError(folly::tag_t<ExecutionError>{}, [](const ExecutionError& e) {
                        LOG(ERROR) << e.what();
                    })
            .onError(folly::tag_t<std::exception>{}, [](const std::exception& e) {
                        LOG(ERROR) << "exception: " << e.what();
                    })
            .ensure([this]() {
                auto us = duration_cast<microseconds>(watch_.elapsed());
                LOG(INFO) << "elapsed time: " << us.count() << "us";
            });
    }

protected:
    folly::stop_watch<> watch_;
    ExecutionPlan* plan_;
    std::unique_ptr<QueryContext> qctx_;
    std::unique_ptr<Scheduler> scheduler_;
};

TEST_F(ExecutionPlanTest, TestSimplePlan) {
    auto start = StartNode::make(qctx_.get());
    Expression* expr = nullptr;
    auto filter = Filter::make(qctx_.get(), start, expr);
    auto dedup = Dedup::make(qctx_.get(), filter, nullptr);
    YieldColumns* cols = nullptr;
    auto proj = Project::make(qctx_.get(), dedup, cols);
    plan_->setRoot(proj);

    run();
}

TEST_F(ExecutionPlanTest, TestSelect) {
    auto start = StartNode::make(qctx_.get());
    auto thenStart = StartNode::make(qctx_.get());
    auto filter = Filter::make(qctx_.get(), thenStart, nullptr);
    auto elseStart = StartNode::make(qctx_.get());
    auto project = Project::make(qctx_.get(), elseStart, nullptr);
    auto select = Selector::make(qctx_.get(), start, filter, project, nullptr);
    auto output = Project::make(qctx_.get(), select, nullptr);

    plan_->setRoot(output);

    run();
}

TEST_F(ExecutionPlanTest, TestLoopPlan) {
    auto start = StartNode::make(qctx_.get());
    auto bodyStart = StartNode::make(qctx_.get());
    auto filter = Filter::make(qctx_.get(), bodyStart, nullptr);
    auto loop = Loop::make(qctx_.get(), start, filter, nullptr);
    auto project = Project::make(qctx_.get(), loop, nullptr);

    plan_->setRoot(project);

    run();
}

TEST_F(ExecutionPlanTest, TestMultiOutputs) {
    auto start = StartNode::make(qctx_.get());
    auto mout = MultiOutputsNode::make(qctx_.get(), start);
    auto filter = Filter::make(qctx_.get(), mout, nullptr);
    auto project = Project::make(qctx_.get(), mout, nullptr);
    auto uni = Union::make(qctx_.get(), filter, project);
    auto output = Project::make(qctx_.get(), uni, nullptr);

    plan_->setRoot(output);

    run();
}

TEST_F(ExecutionPlanTest, TestMultiOutputsInLoop) {
    auto loopStart = StartNode::make(qctx_.get());
    auto mout = MultiOutputsNode::make(qctx_.get(), loopStart);
    auto filter = Filter::make(qctx_.get(), mout, nullptr);
    auto project = Project::make(qctx_.get(), mout, nullptr);
    auto uni = Union::make(qctx_.get(), filter, project);
    auto loopEnd = Project::make(qctx_.get(), uni, nullptr);
    auto start = StartNode::make(qctx_.get());
    auto loop = Loop::make(qctx_.get(), start, loopEnd, nullptr);
    auto end = Project::make(qctx_.get(), loop, nullptr);

    plan_->setRoot(end);

    run();
}

}   // namespace graph
}   // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
