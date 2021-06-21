/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "scheduler/Scheduler.h"

#include "context/QueryContext.h"
#include "executor/ExecutionError.h"
#include "executor/Executor.h"
#include "executor/logic/LoopExecutor.h"
#include "executor/logic/PassThroughExecutor.h"
#include "executor/logic/SelectExecutor.h"
#include "planner/plan/Logic.h"
#include "planner/plan/PlanNode.h"
#include "planner/plan/Query.h"

namespace nebula {
namespace graph {

/*static*/ void Scheduler::analyzeLifetime(const PlanNode* root, bool inLoop) {
    std::stack<std::tuple<const PlanNode*, bool>> stack;
    stack.push(std::make_tuple(root, inLoop));
    while (!stack.empty()) {
        const auto& current = stack.top();
        const auto* currentNode = std::get<0>(current);
        const auto currentInLoop = std::get<1>(current);
        for (auto& inputVar : currentNode->inputVars()) {
            if (inputVar != nullptr) {
                inputVar->setLastUser(
                    (currentNode->kind() == PlanNode::Kind::kLoop || currentInLoop)
                        ? -1
                        : currentNode->id());
            }
        }
        stack.pop();

        for (auto dep : currentNode->dependencies()) {
            stack.push(std::make_tuple(dep, currentInLoop));
        }
        switch (currentNode->kind()) {
            case PlanNode::Kind::kSelect: {
                auto sel = static_cast<const Select*>(currentNode);
                stack.push(std::make_tuple(sel->then(), currentInLoop));
                stack.push(std::make_tuple(sel->otherwise(), currentInLoop));
                break;
            }
            case PlanNode::Kind::kLoop: {
                auto loop = static_cast<const Loop*>(currentNode);
                loop->outputVarPtr()->setLastUser(-1);
                stack.push(std::make_tuple(loop->body(), true));
                break;
            }
            default:
                break;
        }
    }
}

}   // namespace graph
}   // namespace nebula
