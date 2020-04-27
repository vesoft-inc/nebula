/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/ExecutionPlan.h"

#include <folly/init/Init.h>
#include <folly/stop_watch.h>
#include <gtest/gtest.h>

#include "exec/ExecutionError.h"
#include "exec/Executor.h"
#include "planner/Query.h"
#include "service/ExecutionContext.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;

namespace nebula {
namespace graph {

class ExecutionPlanTest : public ::testing::Test {
public:
    void SetUp() override {
        ectx_ = std::make_unique<ExecutionContext>();
        plan_ = std::make_unique<ExecutionPlan>(ectx_.get());
    }

    void cleanup() {
        ASSERT_TRUE(plan_);

        plan_.reset();
    }

    void run() {
        ASSERT_NE(plan_->root(), nullptr);

        Executor* executor = plan_->createExecutor();
        ASSERT_NE(executor, nullptr);

        auto status = executor->prepare();
        ASSERT(status.ok());

        watch_.reset();
        auto future =
            executor->execute()
                .then([](Status s) { ASSERT_TRUE(s.ok()) << s.toString(); })
                .onError([](const ExecutionError& e) { LOG(INFO) << e.what(); })
                .onError([](const std::exception& e) { LOG(INFO) << "exception: " << e.what(); })
                .ensure([this]() {
                    auto us = duration_cast<microseconds>(watch_.elapsed());
                    LOG(INFO) << "elapsed time: " << us.count() << "us";
                    cleanup();
                });

        std::move(future).get();
    }

protected:
    folly::stop_watch<> watch_;
    std::unique_ptr<ExecutionPlan> plan_;
    std::unique_ptr<ExecutionContext> ectx_;
};

TEST_F(ExecutionPlanTest, TestSimplePlan) {
    auto start = StartNode::make(plan_.get());
    Expression* expr = nullptr;
    auto filter = Filter::make(plan_.get(), start, expr);
    auto dedup = Dedup::make(plan_.get(), filter, nullptr);
    YieldColumns* cols = nullptr;
    auto proj = Project::make(plan_.get(), dedup, cols);
    plan_->setRoot(proj);

    run();
}

TEST_F(ExecutionPlanTest, TestSelect) {
    auto start = StartNode::make(plan_.get());
    auto thenStart = StartNode::make(plan_.get());
    auto filter = Filter::make(plan_.get(), thenStart, nullptr);
    auto elseStart = StartNode::make(plan_.get());
    auto project = Project::make(plan_.get(), elseStart, nullptr);
    auto select = Selector::make(plan_.get(), start, filter, project, nullptr);
    auto output = Project::make(plan_.get(), select, nullptr);

    plan_->setRoot(output);

    run();
}

TEST_F(ExecutionPlanTest, TestLoopPlan) {
    auto start = StartNode::make(plan_.get());
    auto bodyStart = StartNode::make(plan_.get());
    auto filter = Filter::make(plan_.get(), bodyStart, nullptr);
    auto loop = Loop::make(plan_.get(), start, filter, nullptr);
    auto project = Project::make(plan_.get(), loop, nullptr);

    plan_->setRoot(project);

    run();
}

TEST_F(ExecutionPlanTest, TestMutiOutputs) {
    auto start = StartNode::make(plan_.get());
    auto mout = MultiOutputsNode::make(plan_.get(), start);
    auto filter = Filter::make(plan_.get(), mout, nullptr);
    auto project = Project::make(plan_.get(), mout, nullptr);
    auto uni = Union::make(plan_.get(), filter, project);
    auto output = Project::make(plan_.get(), uni, nullptr);

    plan_->setRoot(output);

    run();
}

TEST_F(ExecutionPlanTest, TestMutiOutputsInLoop) {
    auto loopStart = StartNode::make(plan_.get());
    auto mout = MultiOutputsNode::make(plan_.get(), loopStart);
    auto filter = Filter::make(plan_.get(), mout, nullptr);
    auto project = Project::make(plan_.get(), mout, nullptr);
    auto uni = Union::make(plan_.get(), filter, project);
    auto loopEnd = Project::make(plan_.get(), uni, nullptr);
    auto start = StartNode::make(plan_.get());
    auto loop = Loop::make(plan_.get(), start, loopEnd, nullptr);
    auto end = Project::make(plan_.get(), loop, nullptr);

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
