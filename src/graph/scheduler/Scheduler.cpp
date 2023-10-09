/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/scheduler/Scheduler.h"

#include <atomic>
#include <limits>

#include "graph/context/QueryContext.h"
#include "graph/executor/ExecutionError.h"
#include "graph/executor/Executor.h"
#include "graph/executor/logic/LoopExecutor.h"
#include "graph/executor/logic/PassThroughExecutor.h"
#include "graph/executor/logic/SelectExecutor.h"
#include "graph/planner/plan/Logic.h"
#include "graph/planner/plan/PlanNode.h"
#include "graph/planner/plan/Query.h"

namespace nebula {
namespace graph {

/*static*/ void Scheduler::analyzeLifetime(const PlanNode* root, std::size_t loopLayers) {
  std::stack<std::tuple<const PlanNode*, std::size_t>> stack;
  std::unordered_set<const PlanNode*> visited;
  stack.push(std::make_tuple(root, loopLayers));
  while (!stack.empty()) {
    const auto& current = stack.top();
    const auto* currentNode = std::get<0>(current);
    const auto currentLoopLayers = std::get<1>(current);
    for (auto& inputVar : currentNode->inputVars()) {
      if (inputVar != nullptr) {
        inputVar->userCount.fetch_add(1, std::memory_order_relaxed);
      }
    }
    auto* currentMutNode = const_cast<PlanNode*>(currentNode);
    currentMutNode->setLoopLayers(currentLoopLayers);
    stack.pop();
    if (!visited.emplace(currentNode).second) {
      continue;
    }

    for (auto dep : currentNode->dependencies()) {
      stack.push(std::make_tuple(dep, currentLoopLayers));
    }
    switch (currentNode->kind()) {
      case PlanNode::Kind::kSelect: {
        auto sel = static_cast<const Select*>(currentNode);
        // used by scheduler
        sel->outputVarPtr()->userCount.store(std::numeric_limits<uint64_t>::max(),
                                             std::memory_order_relaxed);
        stack.push(std::make_tuple(sel->then(), currentLoopLayers));
        stack.push(std::make_tuple(sel->otherwise(), currentLoopLayers));
        break;
      }
      case PlanNode::Kind::kLoop: {
        auto loop = const_cast<Loop*>(static_cast<const Loop*>(currentNode));
        // used by scheduler
        loop->outputVarPtr()->userCount.store(std::numeric_limits<uint64_t>::max(),
                                              std::memory_order_relaxed);
        loop->setLoopLayers(currentLoopLayers + 1);
        stack.push(std::make_tuple(loop->body(), loop->loopLayers()));
        break;
      }
      default:
        break;
    }
  }
}

}  // namespace graph
}  // namespace nebula
